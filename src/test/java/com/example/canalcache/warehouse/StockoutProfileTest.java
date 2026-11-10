package com.example.canalcache.warehouse;

import com.example.canalcache.platform.ArchiveTestFixture;
import com.example.canalcache.platform.PlatformProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import javax.sql.DataSource;

import static org.assertj.core.api.Assertions.assertThat;

class StockoutProfileTest {
    private final DataSource db = ArchiveTestFixture.database("stockout_profile");
    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withInitializer(new ConfigDataApplicationContextInitializer())
            .withPropertyValues("spring.config.location=classpath:/")
            .withUserConfiguration(StockoutController.class, StockoutStore.class)
            .withBean("dataSource", DataSource.class, () -> db)
            .withBean(ObjectMapper.class, ObjectMapper::new);

    @Test void platformEnabledAloneDoesNotLoadExampleOrRequireItsTable() {
        runner.withPropertyValues("platform.enabled=true").run(context -> {
            assertThat(context).hasNotFailed().doesNotHaveBean(StockoutController.class).doesNotHaveBean(StockoutStore.class);
            var properties = Binder.get(context.getEnvironment()).bind("platform", PlatformProperties.class).get();
            assertThat(properties.getEnabledExamples()).isEmpty();
            assertThat(properties.getTargets()).doesNotContainKey("stockout-workbench");
        });
    }

    @Test void explicitProfileLoadsExampleAndItsTargetAndPersistsTasks() throws Exception {
        ArchiveTestFixture.initializeStockout(db);
        runner.withPropertyValues("spring.profiles.active=stockout-demo", "platform.enabled=true", "server.port=18081").run(context -> {
            assertThat(context).hasNotFailed().hasSingleBean(StockoutController.class).hasSingleBean(StockoutStore.class);
            var properties = Binder.get(context.getEnvironment()).bind("platform", PlatformProperties.class).get();
            assertThat(properties.getEnabledExamples()).containsExactly("stockout");
            assertThat(properties.getTargets().get("stockout-workbench").getUrl())
                    .isEqualTo("http://localhost:18081/warehouse/stockout-events");
            assertThat(context.getBean(StockoutStore.class).receive("a".repeat(64), 1, "SKU-1", 1700000000000L).status())
                    .isEqualTo("PENDING");
        });
    }

    @Test void profileCannotOverrideAnExplicitlyDisabledPlatform() {
        runner.withPropertyValues("spring.profiles.active=stockout-demo", "platform.enabled=false").run(context ->
                assertThat(context).hasNotFailed().doesNotHaveBean(StockoutController.class).doesNotHaveBean(StockoutStore.class));
    }
}
