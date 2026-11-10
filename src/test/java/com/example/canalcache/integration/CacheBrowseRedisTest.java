package com.example.canalcache.integration;

import com.example.canalcache.controller.DepartmentController;
import com.example.canalcache.controller.UserController;
import com.example.canalcache.entity.Department;
import com.example.canalcache.entity.User;
import com.example.canalcache.loader.DepartmentCacheLoader;
import com.example.canalcache.loader.UserCacheLoader;
import com.example.canalcache.mapper.DepartmentMapper;
import com.example.canalcache.mapper.UserMapper;
import com.example.canalcache.service.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import java.util.*;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@EnabledIfSystemProperty(named = "cache.test.redis.host", matches = ".+")
class CacheBrowseRedisTest {
    private LettuceConnectionFactory factory;
    private StringRedisTemplate redis;
    private CacheAnnotationProcessor processor;
    private String prefix;
    private final List<String> ownedKeys = new ArrayList<>();
    private final SimpleMeterRegistry metrics = new SimpleMeterRegistry();
    private final UserMapper users = mock(UserMapper.class);
    private final DepartmentMapper departments = mock(DepartmentMapper.class);
    private CacheQueryService queries;

    @BeforeEach
    void setUp() {
        factory = CacheFillRedisTest.connectionFactory(System.getProperty("cache.test.redis.host"));
        redis = new StringRedisTemplate(factory);
        prefix = "browse-test:" + UUID.randomUUID() + ":";
        processor = new CacheAnnotationProcessor();
        ReflectionTestUtils.setField(processor, "basePackage", "com.example.canalcache.entity");
        processor.init();
        processor.getConfig("users").setKeyTemplate(prefix + "user:#id");
        processor.getConfig("departments").setKeyTemplate(prefix + "department:#id");
        queries = new CacheQueryService(new CacheLoaderRegistry(List.of(new UserCacheLoader(users),
                new DepartmentCacheLoader(departments)), processor),
                new CachedQueryService(redis, processor, new ObjectMapper(), metrics));
    }

    @AfterEach
    void tearDown() {
        redis.delete(ownedKeys); factory.destroy(); metrics.close();
    }

    @Test
    void realCursorWalkKeepsGroupAndExactIdsWithoutLosingBatchItems() {
        Set<String> expected = new HashSet<>();
        for (long id = 1; id <= 130; id++) {
            String key = own("user:" + id); expected.add(key);
            redis.opsForHash().put(key, "data", "null");
        }
        String max = own("user:" + Long.MAX_VALUE); expected.add(max);
        redis.opsForHash().put(max, "data", "null");
        String loading = own("user:201"); expected.add(loading);
        redis.opsForHash().put(loading, "load_token", "pending");
        redis.expire(loading, java.time.Duration.ofSeconds(30));
        for (String suffix : List.of("stats", "01", "0", "-1", "9223372036854775808"))
            redis.opsForHash().put(own("user:" + suffix), "data", "metadata");
        redis.opsForHash().put(own("department:1"), "data", "other-group");
        redis.opsForValue().set(own("user:999"), "wrong-type");

        CacheBrowseService browser = new CacheBrowseService(redis);
        Set<String> actual = new HashSet<>();
        String cursor = "0";
        int requests = 0;
        do {
            var page = browser.browse("users", processor.getConfig("users"), cursor, 1);
            page.entries().forEach(entry -> {
                assertThat(entry.key()).isEqualTo(prefix + "user:" + entry.id());
                actual.add(entry.key());
            });
            cursor = page.nextCursor();
            assertThat(++requests).as("SCAN must eventually terminate").isLessThan(1000);
        } while (!cursor.equals("0"));
        assertThat(requests).isGreaterThan(1);
        assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    void bothHttpServicesUseRegisteredLoadersAndBatchOnlyMissingIds() throws Exception {
        var mvc = MockMvcBuilders.standaloneSetup(new UserController(new UserQueryService(queries),
                        mock(UserWriteService.class)), new DepartmentController(new DepartmentService(departments, queries))).build();
        User one = CacheFillRedisTest.user(1, "cached"), two = CacheFillRedisTest.user(2, "loaded");
        when(users.selectById(1L)).thenReturn(one);
        ownedKeys.addAll(List.of(prefix + "user:1", prefix + "user:2", prefix + "user:3", prefix + "department:1"));
        mvc.perform(get("/users/1")).andExpect(status().isOk()).andExpect(jsonPath("$.name").value("cached"));
        when(users.selectBatchIds(List.of(2L, 3L))).thenReturn(List.of(two));
        mvc.perform(post("/users/batch").contentType("application/json").content("{\"ids\":[1,2,3,2]}"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.items.length()").value(2))
                .andExpect(jsonPath("$.missingIds[0]").value(3));
        verify(users, times(1)).selectBatchIds(List.of(2L, 3L));
        verify(users, times(1)).selectById(1L);
        Department dept = new Department(); dept.setId(1L); dept.setName("R&D");
        when(departments.selectById(1L)).thenReturn(dept);
        mvc.perform(get("/departments/1")).andExpect(status().isOk()).andExpect(jsonPath("$.name").value("R&D"));
        assertThat(redis.getExpire(prefix + "department:1")).isBetween(1L, 600L);
        assertThat(redis.getExpire(prefix + "user:1")).isBetween(1L, 1800L);
        assertThat(redis.getExpire(prefix + "user:3")).isBetween(1L, 30L);
        assertThat(redis.opsForHash().get(prefix + "user:3", "data")).isEqualTo("null");
    }

    @Test
    void registeredLoaderFailurePropagatesAndNextCallCanLoad() {
        own("user:1");
        when(users.selectById(1L)).thenThrow(new DataAccessResourceFailureException("database unavailable"))
                .thenReturn(CacheFillRedisTest.user(1, "recovered"));
        assertThatThrownBy(() -> queries.getById("users", 1L, User.class))
                .isInstanceOf(DataAccessResourceFailureException.class);
        assertThat(redis.hasKey(prefix + "user:1")).isFalse();
        assertThat(queries.getById("users", 1L, User.class).getName()).isEqualTo("recovered");
    }

    private String own(String suffix) { String key = prefix + suffix; ownedKeys.add(key); return key; }
}
