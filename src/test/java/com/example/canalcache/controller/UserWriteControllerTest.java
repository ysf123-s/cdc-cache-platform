package com.example.canalcache.controller;

import com.baomidou.mybatisplus.core.conditions.Wrapper;
import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.example.canalcache.entity.User;
import com.example.canalcache.mapper.UserMapper;
import com.example.canalcache.service.UserQueryService;
import com.example.canalcache.service.UserWriteService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/**
 * 通过 HTTP 层调用真实写服务，只有数据库 Mapper 被替换。
 * 不代表真实 MySQL 或 Canal 链路已验证。
 */
class UserWriteControllerTest {
    private final UserMapper mapper = mock(UserMapper.class);
    private final UserQueryService queries = mock(UserQueryService.class);
    private final ObjectMapper json = new ObjectMapper();
    private MockMvc mvc;

    @BeforeEach
    void setUp() {
        mvc = MockMvcBuilders.standaloneSetup(
                new UserController(queries, new UserWriteService(mapper))).build();
    }

    @Test
    void createReturnsGeneratedIdAndLocation() throws Exception {
        when(mapper.insert(any(User.class))).thenAnswer(invocation -> {
            User user = invocation.getArgument(0);
            assertThat(user.getId()).isNull();
            assertThat(user.getCreatedAt()).isNull();
            assertThat(user.getName()).isEqualTo("Alice");
            assertThat(user.getEmail()).isEqualTo("alice@example.com");
            user.setId(41L); // 模拟 MyBatis-Plus 回填数据库生成的主键。
            return 1;
        });

        mvc.perform(post("/users").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\":\"Alice\",\"email\":\"alice@example.com\",\"phone\":\"10001\"}"))
                .andExpect(status().isCreated())
                .andExpect(header().string("Location", "/users/41"))
                .andExpect(jsonPath("$.id").value(41));

        verifyNoInteractions(queries);
    }

    @Test
    void putIncludesNullOptionalFieldsInSqlAndOnlyUpdatesProfile() throws Exception {
        when(mapper.update(isNull(), any(Wrapper.class))).thenReturn(1);

        mvc.perform(put("/users/41").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\":\"Alice-new\",\"email\":null}"))
                .andExpect(status().isNoContent())
                .andExpect(content().string(""));

        ArgumentCaptor<UpdateWrapper<User>> update = ArgumentCaptor.forClass(UpdateWrapper.class);
        verify(mapper).update(isNull(), update.capture());
        UpdateWrapper<User> sql = update.getValue();
        assertThat(sql.getSqlSet()).contains("name=", "email=", "phone=")
                .doesNotContain("created_at=", "id=");
        assertThat(sql.getSqlSegment()).contains("id =");
        assertThat(sql.getParamNameValuePairs().values()).containsExactlyInAnyOrder("Alice-new", null, null, 41L);
        verifyNoInteractions(queries);
    }

    @Test
    void updateMissingUserReturns404() throws Exception {
        mvc.perform(put("/users/99").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\":\"Alice\"}"))
                .andExpect(status().isNotFound());
    }

    @Test
    void deleteReturns204AndRepeatedDeleteReturns404() throws Exception {
        when(mapper.deleteById(41L)).thenReturn(1, 0);

        mvc.perform(delete("/users/41")).andExpect(status().isNoContent());
        mvc.perform(delete("/users/41")).andExpect(status().isNotFound());

        verifyNoInteractions(queries);
    }

    @ParameterizedTest
    @ValueSource(strings = {"{}", "{\"name\":\"  \"}", "null", "{broken"})
    void invalidBodyIsRejectedBeforeDatabaseWrite(String body) throws Exception {
        mvc.perform(post("/users").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isBadRequest());
        mvc.perform(put("/users/41").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isBadRequest());

        verifyNoInteractions(mapper);
    }

    @Test
    void fieldsLongerThanDatabaseColumnsAreRejected() throws Exception {
        for (Map<String, String> fields : java.util.List.of(
                Map.of("name", "n".repeat(65)),
                Map.of("name", "Alice", "email", "e".repeat(129)),
                Map.of("name", "Alice", "phone", "1".repeat(33)))) {
            String body = json.writeValueAsString(fields);
            mvc.perform(post("/users").contentType(MediaType.APPLICATION_JSON).content(body))
                    .andExpect(status().isBadRequest());
            mvc.perform(put("/users/41").contentType(MediaType.APPLICATION_JSON).content(body))
                    .andExpect(status().isBadRequest());
        }
        verifyNoInteractions(mapper);
    }

    @Test
    void databaseFailureIsNotReportedAsSuccessfulCreate() {
        when(mapper.insert(any(User.class))).thenThrow(new DataAccessResourceFailureException("MySQL unavailable"));

        assertThatThrownBy(() -> mvc.perform(post("/users").contentType(MediaType.APPLICATION_JSON)
                .content("{\"name\":\"Alice\"}")))
                .hasCauseInstanceOf(DataAccessResourceFailureException.class);
    }
}
