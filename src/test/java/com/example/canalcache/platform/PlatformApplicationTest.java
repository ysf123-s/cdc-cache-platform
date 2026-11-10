package com.example.canalcache.platform;

import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.example.canalcache.platform.history.*;
import com.example.canalcache.service.*;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.*;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import static com.example.canalcache.platform.ArchiveTestFixture.*;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

/** Real Boot scanning, HTTP endpoints, scheduler, relational transactions and localhost HTTP delivery.
 * Kafka and Redis are intentionally mocked; this is not MySQL/Canal/Kafka/Redis end-to-end acceptance. */
@SpringBootTest(webEnvironment=SpringBootTest.WebEnvironment.RANDOM_PORT)
class PlatformApplicationTest {
    static final ArchiveTestFixture fixture;
    static final HttpServer receiver;
    static final AtomicReference<String> received=new AtomicReference<>();
    static {
        try {
            fixture=new ArchiveTestFixture();
            receiver=HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);
            receiver.createContext("/hook",exchange->{
                received.set(new String(exchange.getRequestBody().readAllBytes(),StandardCharsets.UTF_8));
                exchange.sendResponseHeaders(204,-1);exchange.close();
            });
            receiver.start();
        } catch(Exception e){throw new ExceptionInInitializerError(e);}
    }
    @DynamicPropertySource static void properties(DynamicPropertyRegistry p) {
        p.add("platform.enabled",()->"true");
        p.add("platform.database",fixture.properties::getDatabase);
        p.add("platform.tables[0]",()->"inventory");
        p.add("platform.datasource.url",fixture.platform::getURL);
        p.add("platform.datasource.username",()->"sa");
        p.add("platform.datasource.password",()->"");
        p.add("platform.datasource.driver-class-name",()->"org.h2.Driver");
        p.add("spring.datasource.url",fixture.business::getURL);
        p.add("spring.datasource.username",()->"sa");
        p.add("spring.datasource.password",()->"");
        p.add("spring.datasource.driver-class-name",()->"org.h2.Driver");
        p.add("platform.targets.hook.url",()->"http://127.0.0.1:"+receiver.getAddress().getPort()+"/hook");
        p.add("platform.dispatch-delay-ms",()->"50");
    }
    @MockBean CanalConnectorWrapper connector;
    @MockBean ArchiveConsumer consumer;
    @MockBean CanalCacheSyncService cacheSync;
    @Autowired TestRestTemplate http;
    @Autowired ArchiveService archive;
    @Autowired ArchiveStore store;

    @Test void actualApplicationStartsAndPageRequestsProduceAutomaticHttpDelivery() {
        try {
            assertThat(http.getForEntity("/platform-dashboard.html",String.class).getStatusCode()).isEqualTo(HttpStatus.OK);
            assertThat(http.getForEntity("/warehouse/stockout-tasks",String.class).getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
            assertThat(http.getForObject("/platform/examples",List.class)).isEmpty();
            assertThat(http.getForObject("/platform/targets",List.class)).doesNotContain("stockout-workbench");
            var body=Map.of("table","inventory","targetName","hook","operations",List.of("INSERT"));
            var created=http.postForEntity("/platform/subscriptions",body,Map.class);
            assertThat(created.getStatusCode()).isEqualTo(HttpStatus.CREATED);
            String subId=(String)created.getBody().get("subscriptionId");
            archive.archive(fixture.record(88,fixture.rows(EventType.INSERT,inserted("88","actual HTTP path"))));
            await().atMost(Duration.ofSeconds(5)).untilAsserted(()->{
                assertThat(received.get()).contains("\"deliveryId\"","actual HTTP path");
                assertThat(store.listDeliveryTasks("SUCCEEDED",subId,null,10)).hasSize(1);
            });
            var headers=new HttpHeaders();headers.setContentType(MediaType.APPLICATION_JSON);
            assertThat(http.exchange("/platform/subscriptions/"+subId+"/enabled?expectedVersion=1",HttpMethod.PUT,
                    new HttpEntity<>(Map.of("enabled",false),headers),Map.class).getStatusCode()).isEqualTo(HttpStatus.OK);
            assertThat(http.postForEntity("/platform/subscriptions",Map.of("table","inventory","targetName","hook",
                    "operations",List.of("UPDATE"),"filterField","available","filterValue","not-a-number"),Map.class).getStatusCode())
                    .isEqualTo(HttpStatus.BAD_REQUEST);
            assertThat(http.getForEntity("/platform/events?table=inventory",Map.class).getStatusCode()).isEqualTo(HttpStatus.OK);
        } finally {receiver.stop(0);}
    }
}
