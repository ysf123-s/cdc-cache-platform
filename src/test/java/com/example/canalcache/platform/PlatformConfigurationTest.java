package com.example.canalcache.platform;

import com.baomidou.mybatisplus.autoconfigure.MybatisPlusAutoConfiguration;
import com.example.canalcache.mapper.UserMapper;
import com.example.canalcache.parser.BinlogParser;
import com.example.canalcache.platform.history.TableRegistry;
import com.example.canalcache.service.CanalConnectorWrapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.ibatis.session.SqlSessionFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.JdbcTemplateAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class PlatformConfigurationTest {
    private ArchiveTestFixture fixture;
    private ApplicationContextRunner runner;

    @BeforeEach
    void setUp() throws Exception {
        fixture = new ArchiveTestFixture();
        JdbcTemplate business = new JdbcTemplate(fixture.business);
        business.execute("CREATE TABLE users (id BIGINT PRIMARY KEY, name VARCHAR(64), email VARCHAR(128), phone VARCHAR(32), created_at DATETIME)");
        business.update("INSERT INTO users (id,name) VALUES (1,'business database')");
        CanalConnectorWrapper connector = mock(CanalConnectorWrapper.class);
        when(connector.createArchiveConsumer(anyString())).thenThrow(new IllegalStateException("Broker intentionally absent in datasource test"));
        runner = new ApplicationContextRunner().withConfiguration(AutoConfigurations.of(DataSourceAutoConfiguration.class,
                        JdbcTemplateAutoConfiguration.class, DataSourceTransactionManagerAutoConfiguration.class, MybatisPlusAutoConfiguration.class))
                .withUserConfiguration(PlatformConfiguration.class, BusinessMappers.class)
                .withBean(BinlogParser.class, BinlogParser::new).withBean(ObjectMapper.class, ObjectMapper::new)
                .withBean(CanalConnectorWrapper.class, () -> connector)
                .withPropertyValues("spring.datasource.url=" + fixture.business.getURL(), "spring.datasource.username=sa",
                        "spring.datasource.driver-class-name=org.h2.Driver", "platform.database=" + fixture.properties.getDatabase(),
                        "platform.tables[0]=inventory", "platform.datasource.url=" + fixture.platform.getURL(),
                        "platform.datasource.username=sa", "platform.datasource.driver-class-name=org.h2.Driver");
    }

    @Test
    void enablingArchiveKeepsBusinessMapperAndDefaultTransactionsOnPrimaryDatasource() {
        runner.withPropertyValues("platform.enabled=true").run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context.getBeansOfType(DataSource.class)).hasSize(2);
            DataSource primary = context.getBean("dataSource", DataSource.class);
            assertThat(context.getBean(SqlSessionFactory.class).getConfiguration().getEnvironment().getDataSource()).isSameAs(primary);
            assertThat(context.getBean(UserMapper.class).selectById(1L).getName()).isEqualTo("business database");
            assertThat(((DataSourceTransactionManager) context.getBean(PlatformTransactionManager.class)).getDataSource()).isSameAs(primary);
            assertThat(context.getBean(TableRegistry.class).views()).hasSize(1);
        });
    }

    @Test
    void disablingArchiveUsesOriginalSingleDatasourceAndDoesNotCreatePlatformComponents() {
        runner.withPropertyValues("platform.enabled=false").run(context -> {
            assertThat(context).hasNotFailed().hasSingleBean(DataSource.class).doesNotHaveBean(TableRegistry.class);
            assertThat(context.getBean(UserMapper.class).selectById(1L).getName()).isEqualTo("business database");
        });
    }

    @Test
    void flatMessageCannotSilentlyEnableReducedPlatformCapabilities() {
        runner.withPropertyValues("platform.enabled=true", "canal.mq.kafka.flat-message=true").run(context -> {
            assertThat(context).hasFailed();
            assertThat(context.getStartupFailure()).hasRootCauseMessage("Platform archiving requires canal.mq.kafka.flat-message=false");
        });
    }

    @Test
    void sharingCacheGroupIsRejectedBeforeStartingArchiveConsumer() {
        runner.withPropertyValues("platform.enabled=true", "platform.group-id=cache", "canal.mq.kafka.group-id=cache").run(context -> {
            assertThat(context).hasFailed();
            assertThat(context.getStartupFailure()).hasRootCauseMessage("Platform consumer group must differ from cache group");
        });
    }

    @Configuration(proxyBeanMethods = false)
    @MapperScan("com.example.canalcache.mapper")
    static class BusinessMappers { }
}
