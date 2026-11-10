package com.example.canalcache.integration;

import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.example.canalcache.model.BinlogEvent;
import com.example.canalcache.entity.User;
import com.example.canalcache.entity.Department;
import com.example.canalcache.service.CachedQueryService;
import com.example.canalcache.service.CanalConnectorWrapper;
import com.example.canalcache.service.CanalCacheSyncService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.Map;
import java.util.HashMap;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

/**
 * 集成测试：验证 BinlogEvent 触发正确的 Redis 缓存操作。
 * 使用 Testcontainers 拉起真实 Redis，CanalConnectorWrapper mock 掉防止连 Kafka。
 */
@SpringBootTest
@Testcontainers(disabledWithoutDocker = true)
class CanalCacheIntegrationTest {

    @Container
    static GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379);

    @DynamicPropertySource
    static void redisProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port", () -> redis.getMappedPort(6379));
        // 测试容器未启用密码；显式清空，避免继承 application.yml 的演示密码。
        registry.add("spring.data.redis.password", () -> "");
    }

    // mock 掉 Kafka 连接，防止 @PostConstruct 真正连接
    @MockBean
    private CanalConnectorWrapper canalConnectorWrapper;

    @Autowired
    private RedisTemplate<String, String> redisTemplate;

    @Autowired
    private CanalCacheSyncService syncService;

    @Autowired
    private CachedQueryService queries;

    @BeforeEach
    void setUp() {
        // 同步循环走 Entry 模式，测试用例直接调用 handleEvent，不真正连接 Kafka。
        when(canalConnectorWrapper.isFlatMessageMode()).thenReturn(false);

        // 清理测试 key 保证用例隔离
        redisTemplate.delete("user:1");
        redisTemplate.delete("user:2");
        redisTemplate.delete("user:99");
        redisTemplate.delete("department:1");
    }

    @Test
    void deleteEvent_shouldRemoveCacheKey() {
        // Arrange
        Map<String, String> hash = new HashMap<>();
        hash.put("data", "{\"id\":1,\"name\":\"test\"}");
        hash.put("version", "0");
        redisTemplate.opsForHash().putAll("user:1", hash);
        assertThat(redisTemplate.hasKey("user:1")).isTrue();

        BinlogEvent event = BinlogEvent.builder()
                .tableName("users")
                .eventType(EventType.DELETE)
                .beforeData(Map.of("id", "1", "name", "test"))
                .binlogPosition(100L)
                .build();

        // Act
        syncService.handleEvent(event);

        // Assert
        assertThat(redisTemplate.hasKey("user:1")).isFalse();
    }

    @Test
    void updateEvent_deleteStrategy_shouldInvalidateExistingCacheKey() {
        // users 示例使用 DELETE_MODE，更新数据库后删除缓存，下一次查询负责回填。
        Map<String, String> hash = new HashMap<>();
        hash.put("data", "{\"id\":2,\"name\":\"old\"}");
        hash.put("version", "0");
        redisTemplate.opsForHash().putAll("user:2", hash);

        BinlogEvent event = BinlogEvent.builder()
                .tableName("users")
                .eventType(EventType.UPDATE)
                .beforeData(Map.of("id", "2", "name", "old"))
                .afterData(Map.of("id", "2", "name", "new"))
                .binlogPosition(200L)
                .build();

        // Act
        syncService.handleEvent(event);

        assertThat(redisTemplate.hasKey("user:2")).isFalse();
    }

    @Test
    void deleteEvent_keyNotInRedis_shouldBeIdempotent() {
        // 缓存中不存在 key，删除操作幂等，不报错
        BinlogEvent event = BinlogEvent.builder()
                .tableName("users")
                .eventType(EventType.DELETE)
                .beforeData(Map.of("id", "99"))
                .binlogPosition(300L)
                .build();

        syncService.handleEvent(event);
        assertThat(redisTemplate.hasKey("user:99")).isFalse();
    }

    @Test
    void unknownTable_shouldBeIgnoredSilently() {
        // 没有 @AutoCache 注解的表，事件被忽略，不报错
        BinlogEvent event = BinlogEvent.builder()
                .tableName("unknown_table")
                .eventType(EventType.DELETE)
                .beforeData(Map.of("id", "1"))
                .binlogPosition(400L)
                .build();

        syncService.handleEvent(event);
    }

    @Test
    void batchQueriesUseHashPipelineAndCacheMissingIds() {
        redisTemplate.opsForHash().putAll("user:1", Map.of("data", "{\"id\":1,\"name\":\"cached\"}", "version", "0"));
        User loaded = new User(); loaded.setId(2L); loaded.setName("loaded");

        var result = queries.getByIds("users", List.of(2L, 1L, 99L, 2L), User.class, ids -> {
            assertThat(ids).containsExactly(2L, 99L);
            return List.of(loaded);
        }, User::getId);

        assertThat(result.items()).extracting(User::getId).containsExactly(2L, 1L);
        assertThat(result.missingIds()).containsExactly(99L);
        assertThat(redisTemplate.opsForHash().get("user:99", "data")).isEqualTo("null");
        assertThat(redisTemplate.getExpire("user:99")).isBetween(1L, 30L);
        assertThat(redisTemplate.getExpire("user:2")).isBetween(1L, 1800L);
        var cached = queries.getByIds("users", List.of(2L, 1L, 99L), User.class, ids -> {
            throw new AssertionError("second batch should be fully cached");
        }, User::getId);
        assertThat(cached.items()).extracting(User::getId).containsExactly(2L, 1L);
        assertThat(cached.missingIds()).containsExactly(99L);
    }

    @Test
    void departmentUsesOwnTtlAndSameInvalidationFlow() {
        Department department = new Department(); department.setId(1L); department.setName("R&D");
        queries.getById("departments", 1L, Department.class, () -> department);
        assertThat(redisTemplate.getExpire("department:1")).isBetween(1L, 600L);
        syncService.handleEvent(BinlogEvent.builder().tableName("departments").eventType(EventType.UPDATE)
                .afterData(Map.of("id", "1", "name", "updated")).build());
        assertThat(redisTemplate.hasKey("department:1")).isFalse();
    }
}
