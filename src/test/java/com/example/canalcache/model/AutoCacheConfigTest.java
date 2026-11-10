package com.example.canalcache.model;

import com.example.canalcache.annotation.CacheStrategy;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class AutoCacheConfigTest {

    @Test
    void buildsSimpleKeyFromIdTemplate() {
        AutoCacheConfig config = new AutoCacheConfig();
        config.setKeyTemplate("user:#id");
        config.setStrategy(CacheStrategy.REFRESH_MODE);

        assertThat(config.buildCacheKeySimple(42L)).isEqualTo("user:42");
    }
}
