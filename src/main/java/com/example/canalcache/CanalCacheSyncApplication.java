package com.example.canalcache;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

/**
 * Canal + Redis 缓存自动同步组件启动类
 */
@SpringBootApplication
@MapperScan("com.example.canalcache.mapper")
public class CanalCacheSyncApplication {

    public static void main(String[] args) {
        SpringApplication.run(CanalCacheSyncApplication.class, args);
    }

    @Bean
    public ObjectMapper objectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        return mapper;
    }
}
