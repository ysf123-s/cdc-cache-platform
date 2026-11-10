package com.example.canalcache.dto;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

import java.io.IOException;
import java.util.List;

/** 批量查询和指定缓存失效共用的 HTTP 输入边界。 */
public record IdBatchRequest(@JsonDeserialize(contentUsing = IntegerIdDeserializer.class) List<Long> ids) {
    public void validate() {
        if (ids == null || ids.isEmpty() || ids.size() > 100
                || ids.stream().anyMatch(id -> id == null || id <= 0)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "ids must contain 1 to 100 positive integers");
        }
    }

    /** 禁止 Jackson 将 1.5 等显式无效 ID 自动截断成 1。 */
    public static class IntegerIdDeserializer extends JsonDeserializer<Long> {
        @Override
        public Long deserialize(JsonParser parser, DeserializationContext context) throws IOException {
            if (!parser.hasToken(JsonToken.VALUE_NUMBER_INT)) {
                return context.reportInputMismatch(Long.class, "ID must be an integer JSON number");
            }
            return parser.getLongValue();
        }
    }
}
