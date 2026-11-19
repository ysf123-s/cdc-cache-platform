package com.example.canalcache.service;

import com.example.canalcache.controller.UserController;
import com.example.canalcache.entity.User;
import com.example.canalcache.loader.UserCacheLoader;
import com.example.canalcache.mapper.UserMapper;
import com.example.canalcache.model.AutoCacheConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

class CacheIndexQueryTest {
    private static final String KEY = "user-email:a@x.com";
    private final UserMapper mapper = mock(UserMapper.class);
    private final RedisTemplate<String, String> redis = mock(RedisTemplate.class);
    private final HashOperations<String, Object, Object> hashes = mock(HashOperations.class);
    private final CacheAnnotationProcessor processor = mock(CacheAnnotationProcessor.class);
    private final ObjectMapper json = new ObjectMapper();
    private final SimpleMeterRegistry metrics = new SimpleMeterRegistry();
    private CachedQueryService cached;
    private MockMvc mvc;

    @BeforeEach
    void setUp() {
        AutoCacheConfig config = new AutoCacheConfig();
        config.setTable("users");
        config.setKeyTemplate("user:#id");
        config.setIndexKeys(List.of(AutoCacheConfig.IndexKey.parse("user-email:#email")));
        config.setExpireSeconds(1800);
        config.setEntityClass(User.class);
        when(processor.getConfig("users")).thenReturn(config);
        when(processor.getAllTables()).thenReturn(Set.of("users"));
        when(redis.opsForHash()).thenReturn(hashes);
        // BEGIN_LOAD 返回 null 表示仍未命中且已登记 load_token；FILL / CLEANUP 返回 1。
        when(redis.execute(any(RedisScript.class), anyList(), any(Object[].class)))
                .thenAnswer(invocation -> ((RedisScript<?>) invocation.getArgument(0)).getResultType() == String.class
                        ? null : 1L);
        cached = new CachedQueryService(redis, processor, json, metrics);
        UserQueryService users = new UserQueryService(new CacheQueryService(
                new CacheLoaderRegistry(List.of(new UserCacheLoader(mapper)), processor), cached));
        mvc = MockMvcBuilders.standaloneSetup(new UserController(users, mock(UserWriteService.class))).build();
    }

    @Test
    void cachedIdListIsReturnedWithoutQueryingDatabase() {
        when(hashes.get(KEY, "data")).thenReturn("[2,1]");
        Supplier<List<Long>> loader = mock(Supplier.class);

        assertThat(cached.getIndexedIds("users", "email", "a@x.com", loader)).containsExactly(2L, 1L);

        verifyNoInteractions(loader);
        assertThat(metrics.get("cache.index.items").tag("result", "hit").counter().count()).isEqualTo(1);
    }

    @Test
    void missFillsSortedDistinctIdsWithGroupTtl() {
        assertThat(cached.getIndexedIds("users", "email", "a@x.com", () -> List.of(3L, 1L, 3L)))
                .containsExactly(1L, 3L);

        verify(redis).execute(any(RedisScript.class), eq(List.of(KEY)), eq("[1,3]"), eq("1800"), anyString());
        assertThat(metrics.get("cache.index.fills").tag("result", "written").counter().count()).isEqualTo(1);
    }

    @Test
    void emptyResultIsCachedForThirtySeconds() {
        assertThat(cached.getIndexedIds("users", "email", "a@x.com", List::of)).isEmpty();

        verify(redis).execute(any(RedisScript.class), eq(List.of(KEY)), eq("[]"), eq("30"), anyString());
    }

    @Test
    void loaderFailureCleansUpTokenAndCachesNothing() {
        assertThatThrownBy(() -> cached.getIndexedIds("users", "email", "a@x.com", () -> {
            throw new DataAccessResourceFailureException("MySQL unavailable");
        })).isInstanceOf(DataAccessResourceFailureException.class);

        verify(redis).execute(any(RedisScript.class), eq(List.of(KEY)), anyString());
        verify(redis, never()).execute(any(RedisScript.class), anyList(), anyString(), anyString(), anyString());
    }

    @Test
    void corruptedIdListIsReportedInsteadOfHidden() {
        when(hashes.get(KEY, "data")).thenReturn("{\"id\":1}");

        assertThatThrownBy(() -> cached.getIndexedIds("users", "email", "a@x.com", List::of))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("Invalid cache data");
    }

    @Test
    void httpLookupReturnsUsersFromIndexAndEntityCaches() throws Exception {
        when(hashes.get(KEY, "data")).thenReturn("[1]");
        when(hashes.get("user:1", "data")).thenReturn(json.writeValueAsString(user(1, "a@x.com")));

        mvc.perform(get("/users/by-email").param("email", "a@x.com"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(1))
                .andExpect(jsonPath("$[0].id").value(1));
        verifyNoInteractions(mapper);
    }

    @Test
    void userWhoseEmailChangedAfterIndexWasReadIsFilteredOut() throws Exception {
        when(hashes.get(KEY, "data")).thenReturn("[1]");
        when(hashes.get("user:1", "data")).thenReturn(json.writeValueAsString(user(1, "b@x.com")));

        mvc.perform(get("/users/by-email").param("email", "a@x.com"))
                .andExpect(status().isOk()).andExpect(jsonPath("$").isEmpty());
    }

    @Test
    void databaseLookupKeepsOnlyExactEmailMatches() throws Exception {
        // 忽略大小写的排序规则会把 A@X.COM 也查出来；缓存 key 区分大小写，必须过滤掉。
        when(mapper.selectList(any())).thenReturn(List.of(user(1, "a@x.com"), user(2, "A@X.COM")));
        when(hashes.get("user:1", "data")).thenReturn(json.writeValueAsString(user(1, "a@x.com")));

        mvc.perform(get("/users/by-email").param("email", "a@x.com"))
                .andExpect(status().isOk()).andExpect(jsonPath("$[0].id").value(1));

        verify(redis).execute(any(RedisScript.class), eq(List.of(KEY)), eq("[1]"), eq("1800"), anyString());
    }

    @Test
    void blankOrMissingEmailIsRejected() throws Exception {
        mvc.perform(get("/users/by-email").param("email", " ")).andExpect(status().isBadRequest());
        mvc.perform(get("/users/by-email")).andExpect(status().isBadRequest());
        mvc.perform(get("/users/by-email").param("email", "a".repeat(129))).andExpect(status().isBadRequest());
        verifyNoInteractions(mapper, hashes);
    }

    private static User user(long id, String email) {
        User user = new User();
        user.setId(id);
        user.setName("user" + id);
        user.setEmail(email);
        return user;
    }
}
