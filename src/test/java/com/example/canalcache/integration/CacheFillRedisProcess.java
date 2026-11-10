package com.example.canalcache.integration;

import com.example.canalcache.entity.User;
import com.example.canalcache.service.CachedQueryService;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.springframework.data.redis.core.StringRedisTemplate;

/** A real second JVM, coordinated through standard input rather than production debug endpoints. */
public class CacheFillRedisProcess {
    public static void main(String[] args) throws Exception {
        var factory = CacheFillRedisTest.connectionFactory(args[0]);
        var metrics = new SimpleMeterRegistry();
        try {
            var redis = new StringRedisTemplate(factory);
            var config = CacheFillRedisTest.configuration(args[1]);
            var queries = new CachedQueryService(redis, CacheFillRedisTest.processor(config), new ObjectMapper(), metrics);
            User result = queries.getById("users", 1L, User.class, () -> {
                System.out.println("LOAD_READY"); System.out.flush();
                try {
                    if (System.in.read() != 1) throw new AssertionError("parent did not release loader");
                } catch (java.io.IOException e) { throw new IllegalStateException(e); }
                return CacheFillRedisTest.user(1, "child-old");
            });
            if (!"child-old".equals(result.getName())) throw new AssertionError("existing caller result changed");
            if (metrics.get("cache.query.fills").tags("table", "users", "result", "rejected").counter().count() != 1)
                throw new AssertionError("old fill was not rejected");
        } finally { metrics.close(); factory.destroy(); }
    }
}
