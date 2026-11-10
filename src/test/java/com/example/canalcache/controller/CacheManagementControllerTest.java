package com.example.canalcache.controller;

import com.example.canalcache.annotation.CacheStrategy;
import com.example.canalcache.model.AutoCacheConfig;
import com.example.canalcache.model.BatchQueryResult;
import com.example.canalcache.model.SyncStatus;
import com.example.canalcache.service.CacheAnnotationProcessor;
import com.example.canalcache.service.CacheBrowseService;
import com.example.canalcache.service.CanalCacheSyncService;
import com.example.canalcache.service.UserQueryService;
import com.example.canalcache.service.UserWriteService;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

class CacheManagementControllerTest {
    private final CacheAnnotationProcessor processor = mock(CacheAnnotationProcessor.class);
    private final RedisTemplate<String, String> redis = mock(RedisTemplate.class);
    private final HashOperations<String, Object, Object> hashes = mock(HashOperations.class);
    private final CanalCacheSyncService sync = mock(CanalCacheSyncService.class);
    private final UserQueryService queries = mock(UserQueryService.class);
    private final SimpleMeterRegistry metrics = new SimpleMeterRegistry();
    private MockMvc mvc;

    @BeforeEach
    void setUp() {
        AutoCacheConfig config = new AutoCacheConfig();
        config.setKeyTemplate("user:#id"); config.setExpireSeconds(1800);
        config.setStrategy(CacheStrategy.DELETE_MODE);
        when(processor.getConfig("users")).thenReturn(config);
        when(processor.getAllTables()).thenReturn(Set.of("users"));
        when(redis.opsForHash()).thenReturn(hashes);
        when(sync.getStatus()).thenReturn(new SyncStatus(true, 1, 1, null, null));
        mvc = MockMvcBuilders.standaloneSetup(
                new CacheManagementController(processor, redis, metrics, sync, new CacheBrowseService(redis)),
                new UserController(queries, mock(UserWriteService.class))).build();
    }

    @Test
    void overviewIsAvailableWithoutCallingRedis() throws Exception {
        metrics.counter("cache.query.items", "table", "users", "result", "hit").increment(3);
        metrics.counter("cache.query.items", "table", "users", "result", "miss").increment();
        mvc.perform(get("/cache/overview")).andExpect(status().isOk())
                .andExpect(jsonPath("$.groups[0].table").value("users"))
                .andExpect(jsonPath("$.groups[0].hitRate").value(0.75))
                .andExpect(jsonPath("$.groups[0].nullExpireSeconds").value(30))
                .andExpect(jsonPath("$.sync.activeConsumers").value(1));
        verifyNoInteractions(redis);
    }

    @Test
    void detailsReturnHashAndRemainingTtl() throws Exception {
        when(hashes.entries("user:1")).thenReturn(Map.of("data", "null", "version", "0"));
        when(redis.getExpire("user:1")).thenReturn(25L);
        mvc.perform(get("/cache/entries/users/1")).andExpect(status().isOk())
                .andExpect(jsonPath("$.key").value("user:1"))
                .andExpect(jsonPath("$.hash.data").value("null"))
                .andExpect(jsonPath("$.ttlSeconds").value(25));
    }

    @Test
    void detailIdRemainsExactBeyondJavascriptSafeIntegerRange() throws Exception {
        when(hashes.entries("user:9223372036854775807")).thenReturn(Map.of("data", "null"));
        when(redis.getExpire("user:9223372036854775807")).thenReturn(20L);
        mvc.perform(get("/cache/entries/users/9223372036854775807"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.id").value("9223372036854775807"));
    }

    @Test
    void invalidationBuildsOnlyRegisteredKeysAndReportsActualDeleteCount() throws Exception {
        when(redis.delete(List.of("user:1", "user:2"))).thenReturn(1L);
        mvc.perform(post("/cache/entries/users/invalidate").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"ids\":[1,2,1]}"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.requestedKeys").value(2))
                .andExpect(jsonPath("$.deletedKeys").value(1));
        verify(redis).delete(List.of("user:1", "user:2"));
    }

    @Test
    void unknownTableCannotReadOrDeleteArbitraryKeys() throws Exception {
        mvc.perform(get("/cache/entries/other/1")).andExpect(status().isNotFound());
        mvc.perform(post("/cache/entries/other/invalidate").contentType(MediaType.APPLICATION_JSON)
                .content("{\"ids\":[1]}" )).andExpect(status().isNotFound());
        verifyNoInteractions(redis);
    }

    @ParameterizedTest
    @ValueSource(strings = {"{}", "{\"ids\":null}", "{\"ids\":[]}", "{\"ids\":[0]}",
            "{\"ids\":[-1]}", "{\"ids\":[null]}", "{\"ids\":[1.5]}",
            "{\"ids\":[\"1\"]}", "{\"ids\":[9223372036854775808]}"})
    void invalidBatchInputCannotReachQueriesOrRedis(String body) throws Exception {
        mvc.perform(post("/users/batch").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isBadRequest());
        mvc.perform(post("/cache/entries/users/invalidate").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isBadRequest());
        verifyNoInteractions(queries, redis);
    }

    @Test
    void tooManyIdsAreRejected() throws Exception {
        String body = "{\"ids\":[" + String.join(",", java.util.Collections.nCopies(101, "1")) + "]}";
        mvc.perform(post("/users/batch").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isBadRequest());
        verifyNoInteractions(queries);
    }

    @Test
    void validBatchReportsMissingIds() throws Exception {
        when(queries.getByIds(List.of(99L))).thenReturn(new BatchQueryResult<>(List.of(), List.of(99L)));
        mvc.perform(post("/users/batch").contentType(MediaType.APPLICATION_JSON).content("{\"ids\":[99]}"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.items").isEmpty())
                .andExpect(jsonPath("$.missingIds[0]").value(99));
    }

    @Test
    void redisDeletionFailureIsNotReportedAsSuccess() {
        when(redis.delete(List.of("user:1"))).thenThrow(new RedisConnectionFailureException("offline"));
        assertThatThrownBy(() -> mvc.perform(post("/cache/entries/users/invalidate")
                .contentType(MediaType.APPLICATION_JSON).content("{\"ids\":[1]}")))
                .hasRootCauseInstanceOf(RedisConnectionFailureException.class);
    }

    @Test
    void browseKeepsUnsignedCursorAndExactLongIdsAndFiltersMetadata() throws Exception {
        when(redis.execute(any(RedisScript.class), anyList(), any(Object[].class)))
                .thenReturn(List.of("18446744073709551615", List.of("user:1", "user:9223372036854775807",
                        "user:stats", "user:01", "user:-1", "user:9223372036854775808")));
        mvc.perform(get("/cache/groups/users/entries"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.nextCursor").value("18446744073709551615"))
                .andExpect(jsonPath("$.entries.length()").value(2))
                .andExpect(jsonPath("$.entries[1].id").value("9223372036854775807"));
        verify(redis).execute(any(RedisScript.class), eq(List.of()), eq("0"), eq("user:*"), eq("20"));
    }

    @Test
    void emptyBatchWithNonzeroCursorIsNotEndOfScan() throws Exception {
        when(redis.execute(any(RedisScript.class), anyList(), any(Object[].class)))
                .thenReturn(List.of("123", List.of()));
        mvc.perform(get("/cache/groups/users/entries").param("cursor", "18446744073709551615").param("count", "1"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.entries").isEmpty())
                .andExpect(jsonPath("$.nextCursor").value("123"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "-1", "01", "1.2", "abc", "18446744073709551616", "99999999999999999999999"})
    void invalidCursorCannotReachRedis(String cursor) throws Exception {
        mvc.perform(get("/cache/groups/users/entries").param("cursor", cursor))
                .andExpect(status().isBadRequest());
        verifyNoInteractions(redis);
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "0", "-1", "101", "1.5", "abc", "9999999999999"})
    void invalidCountCannotReachRedis(String count) throws Exception {
        mvc.perform(get("/cache/groups/users/entries").param("count", count))
                .andExpect(status().isBadRequest());
        verifyNoInteractions(redis);
    }

    @Test
    void unknownBrowseGroupIsRejected() throws Exception {
        mvc.perform(get("/cache/groups/other/entries")).andExpect(status().isNotFound());
        verifyNoInteractions(redis);
    }

    @Test
    void scanFailureIsNotReturnedAsEmptyPage() {
        when(redis.execute(any(RedisScript.class), anyList(), any(Object[].class)))
                .thenThrow(new RedisConnectionFailureException("offline"));
        assertThatThrownBy(() -> mvc.perform(get("/cache/groups/users/entries")))
                .hasRootCauseInstanceOf(RedisConnectionFailureException.class);
    }
}
