package com.example.canalcache.service;

import com.example.canalcache.model.AutoCacheConfig;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;

import java.util.Collection;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class CacheNamespacePurgerTest {
    private final RedisTemplate<String, String> redis = mock(RedisTemplate.class);

    @Test
    void purgeClearsPrimaryEntriesAndEveryIndexNamespaceAcrossCursorPages() {
        AutoCacheConfig config = new AutoCacheConfig();
        config.setKeyTemplate("user:#id");
        config.setIndexKeys(List.of(AutoCacheConfig.IndexKey.parse("user-email:#email")));
        when(redis.execute(any(RedisScript.class), eq(List.of()), eq("0"), eq("user:*"), eq("500")))
                .thenReturn(List.of("0", List.of("user:1", "user:stats")));
        when(redis.execute(any(RedisScript.class), eq(List.of()), eq("0"), eq("user-email:*"), eq("500")))
                .thenReturn(List.of("7", List.of("user-email:a@x.com")));
        when(redis.execute(any(RedisScript.class), eq(List.of()), eq("7"), eq("user-email:*"), eq("500")))
                .thenReturn(List.of("0", List.of("user-email:b@x.com")));
        when(redis.unlink(anyCollection())).thenAnswer(invocation -> (long) ((Collection<?>) invocation.getArgument(0)).size());

        long deleted = new CacheNamespacePurger(redis, new CacheBrowseService(redis)).purge("users", config);

        assertThat(deleted).isEqualTo(3);
        verify(redis).unlink(List.of("user:1"));
        verify(redis).unlink(List.of("user-email:a@x.com"));
        verify(redis).unlink(List.of("user-email:b@x.com"));
    }
}
