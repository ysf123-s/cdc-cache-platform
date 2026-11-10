package com.example.canalcache.platform;

import com.example.canalcache.parser.BinlogParser;
import com.example.canalcache.platform.history.*;
import com.example.canalcache.platform.recovery.RecoveryService;
import com.example.canalcache.platform.replay.ReplayService;
import com.example.canalcache.platform.subscription.DeliveryExecutor;
import com.example.canalcache.service.CanalConnectorWrapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.sql.SQLException;

@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(prefix = "platform", name = "enabled", havingValue = "true")
@EnableConfigurationProperties(PlatformProperties.class)
@EnableScheduling
public class PlatformConfiguration {
    @Bean
    public ThreadPoolTaskScheduler taskScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(2);
        scheduler.setThreadNamePrefix("platform-task-");
        return scheduler;
    }
    @Bean
    @Primary
    @ConfigurationProperties("spring.datasource")
    public DataSourceProperties businessDataSourceProperties() { return new DataSourceProperties(); }

    @Bean(name = "dataSource")
    @Primary
    @ConfigurationProperties("spring.datasource.hikari")
    public HikariDataSource businessDataSource(
            @Qualifier("businessDataSourceProperties") DataSourceProperties properties) {
        return properties.initializeDataSourceBuilder().type(HikariDataSource.class).build();
    }

    @Bean
    @ConfigurationProperties("platform.datasource")
    public DataSourceProperties platformDataSourceProperties() { return new DataSourceProperties(); }

    @Bean
    @ConfigurationProperties("platform.datasource.hikari")
    public HikariDataSource platformDataSource(
            @Qualifier("platformDataSourceProperties") DataSourceProperties properties) {
        return properties.initializeDataSourceBuilder().type(HikariDataSource.class).build();
    }

    @Bean
    public ArchiveStore archiveStore(@Qualifier("platformDataSource") DataSource platform, ObjectMapper json) {
        return new ArchiveStore(new JdbcTemplate(platform), json);
    }

    @Bean
    public TableRegistry platformTableRegistry(PlatformProperties properties,
            ArchiveStore store, @Qualifier("dataSource") DataSource business,
            @Qualifier("platformDataSource") DataSource platform,
            @Value("${canal.mq.kafka.flat-message:false}") boolean flatMessage,
            @Value("${canal.mq.kafka.group-id:canal-cache-sync-mq}") String cacheGroup) throws SQLException {
        properties.validate(flatMessage, cacheGroup);
        return TableRegistry.capture(properties, store, business, platform);
    }

    @Bean
    public ArchiveService archiveService(PlatformProperties properties,
            TableRegistry tables, BinlogParser parser, ArchiveStore store,
            @Qualifier("platformDataSource") DataSource platform) {
        TransactionTemplate transaction = new TransactionTemplate(new DataSourceTransactionManager(platform));
        transaction.setTimeout(20);
        return new ArchiveService(properties, tables, parser, store, transaction);
    }

    @Bean
    public RecoveryService recoveryService(PlatformProperties properties, TableRegistry tables,
            ArchiveStore store, @Qualifier("dataSource") DataSource business, ObjectMapper json) {
        return new RecoveryService(properties, tables, store, new JdbcTemplate(business), json);
    }

    @Bean
    public DeliveryExecutor deliveryExecutor(PlatformProperties properties, ArchiveStore store, ObjectMapper json) {
        return new DeliveryExecutor(properties, store, null, json);
    }

    @Bean
    public ReplayService replayService(PlatformProperties properties, ArchiveStore store) {
        return new ReplayService(properties, store);
    }

    @Bean(initMethod = "start", destroyMethod = "close")
    public ArchiveConsumer archiveConsumer(CanalConnectorWrapper connector, ArchiveService archive,
            PlatformProperties properties) {
        return new ArchiveConsumer(connector, archive, properties.getGroupId());
    }
}
