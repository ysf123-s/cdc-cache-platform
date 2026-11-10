package com.example.canalcache.platform;

import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.example.canalcache.model.BinlogColumn;
import com.example.canalcache.model.BinlogEvent;
import com.example.canalcache.platform.subscription.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.*;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import java.sql.Types;
import java.util.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

class SubscriptionRepairTest {
    ArchiveTestFixture f;
    MockMvc mvc;
    @BeforeEach void setup() throws Exception {
        f=new ArchiveTestFixture();
        for(String name:List.of("first","second")) {
            var target=new PlatformProperties.TargetConfig();target.setUrl("http://localhost:8080/"+name);
            f.properties.getTargets().put(name,target);
        }
        mvc=MockMvcBuilders.standaloneSetup(new SubscriptionController(f.properties,f.tables,f.store,mock(DeliveryExecutor.class)))
                .setControllerAdvice(new PlatformExceptionHandler()).build();
    }
    @Test void putReplacesRulesAndActivationUsesItsOwnVersionedEndpoint() throws Exception {
        var request=new EventSubscription.Request(null,null,"inventory","first",List.of("UPDATE"),List.of("available"),"available","0",true);
        String created=mvc.perform(post("/platform/subscriptions").contentType(MediaType.APPLICATION_JSON).content(f.store.encode(request)))
                .andExpect(status().isCreated()).andReturn().getResponse().getContentAsString();
        String id=new ObjectMapper().readTree(created).get("subscriptionId").asText();
        var replacement=new EventSubscription.Request(null,null,"inventory","second",List.of("INSERT"),null,null,null,true);
        mvc.perform(put("/platform/subscriptions/"+id).param("expectedVersion","1").contentType(MediaType.APPLICATION_JSON).content(f.store.encode(replacement)))
                .andExpect(status().isOk()).andExpect(jsonPath("$.targetName").value("second")).andExpect(jsonPath("$.ruleVersion").value(2));
        var stored=f.store.findSubscription(id);
        assertThat(stored.changedFields()).isNull();assertThat(stored.filterField()).isNull();assertThat(stored.filterValue()).isNull();
        mvc.perform(put("/platform/subscriptions/"+id+"/enabled").param("expectedVersion","2")
                .contentType(MediaType.APPLICATION_JSON).content("{\"enabled\":false}"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.enabled").value(false)).andExpect(jsonPath("$.ruleVersion").value(3));
        mvc.perform(put("/platform/subscriptions/"+id+"/enabled").param("expectedVersion","2")
                .contentType(MediaType.APPLICATION_JSON).content("{\"enabled\":true}"))
                .andExpect(status().isConflict());
        assertThat(f.store.findSubscription(id).enabled()).isFalse();
    }
    @Test void invalidTypedFiltersAndAmbiguousRulesReturn400() throws Exception {
        for(var request:List.of(
                new EventSubscription.Request(null,null,"inventory","first",List.of("UPDATE"),null,"available","2147483648",true),
                new EventSubscription.Request(null,null,"inventory","first",List.of("UPDATE"),null,"id","not-a-number",true),
                new EventSubscription.Request(null,null,"inventory","first",List.of("delete"),null,"available","0",true),
                new EventSubscription.Request(null,null,"inventory","first",List.of("UPDATE","update"),null,null,null,true))) {
            mvc.perform(post("/platform/subscriptions").contentType(MediaType.APPLICATION_JSON).content(f.store.encode(request))).andExpect(status().isBadRequest());
        }
        assertThat(f.store.listSubscriptions(f.properties.getSourceId(),f.properties.getDatabase())).isEmpty();
    }
    @Test void allChangedFieldsAreRequiredAndNumericFilterIsComparedByType() {
        var sub=new EventSubscription("sub",f.properties.getSourceId(),f.properties.getDatabase(),"inventory","first","UPDATE",
                "name,available","available","000",true,1,null,null);
        var event=BinlogEvent.builder().databaseName(f.properties.getDatabase()).tableName("inventory").eventType(EventType.UPDATE)
                .beforeData(Map.of("name","before","available","1")).afterData(Map.of("name","after","available","0"))
                .afterColumns(List.of(new BinlogColumn(2,"available",Types.INTEGER,"int",false,false,true,"0"))).build();
        assertThat(SubscriptionMatcher.matches(event,sub)).isTrue();
        event.setBeforeData(Map.of("name","after","available","1"));
        assertThat(SubscriptionMatcher.matches(event,sub)).isFalse();
    }
    @Test void textFilterPreservesWhitespace() {
        var sub=new EventSubscription("sub",f.properties.getSourceId(),f.properties.getDatabase(),"inventory","first","INSERT",null,"name","a ",true,1,null,null);
        var event=BinlogEvent.builder().databaseName(f.properties.getDatabase()).tableName("inventory").eventType(EventType.INSERT)
                .afterData(Map.of("name","a")).afterColumns(List.of(new BinlogColumn(1,"name",Types.VARCHAR,"varchar(64)",false,false,true,"a"))).build();
        assertThat(SubscriptionMatcher.matches(event,sub)).isFalse();
        event.setAfterData(Map.of("name","a "));
        assertThat(SubscriptionMatcher.matches(event,sub)).isTrue();
    }
}
