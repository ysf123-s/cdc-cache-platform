package com.example.canalcache.service;

import com.example.canalcache.model.BinlogEvent;
import com.example.canalcache.model.UnprocessableEventException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.Limit;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 死信：确定无法处理的事件存入 Redis Stream，消费者随后继续处理后续消息。
 *
 * <p>只在整批处理成功、提交位点之前写入，批次回滚重试时不会重复写入；
 * 写入失败（Redis 不可用）会让整批回滚，因此死信不会丢。Stream 近似保留最近 {@value #MAX_ENTRIES} 条。</p>
 */
@Slf4j
@Component
public class DeadLetterStore {
    public static final String STREAM_KEY = "canal-cache:dead-letters";
    public static final String METRIC = "canal.events.dead_lettered";
    static final long MAX_ENTRIES = 10_000;
    static final int MAX_VALUE_CHARS = 4096;
    private static final DefaultRedisScript<String> APPEND = new DefaultRedisScript<>("""
            return redis.call('XADD', KEYS[1], 'MAXLEN', '~', ARGV[1], '*', unpack(ARGV, 2))
            """, String.class);

    private final RedisTemplate<String, String> redis;
    private final ObjectMapper objectMapper;
    private final MeterRegistry registry;
    private final String streamKey;

    @Autowired
    public DeadLetterStore(RedisTemplate<String, String> redis, ObjectMapper objectMapper, MeterRegistry registry) {
        this(redis, objectMapper, registry, STREAM_KEY);
    }

    /** 指定 Stream key，供共享 Redis 上的隔离测试使用。 */
    public DeadLetterStore(RedisTemplate<String, String> redis, ObjectMapper objectMapper, MeterRegistry registry,
                           String streamKey) {
        this.redis = redis;
        this.objectMapper = objectMapper;
        this.registry = registry;
        this.streamKey = streamKey;
    }

    public void recordAll(List<DeadLetter> deadLetters) {
        for (DeadLetter deadLetter : deadLetters) {
            List<String> args = new ArrayList<>();
            args.add(String.valueOf(MAX_ENTRIES));
            fields(deadLetter).forEach((name, value) -> {
                args.add(name);
                args.add(value);
            });
            String id = redis.execute(APPEND, List.of(streamKey), args.toArray());
            registry.counter(METRIC, "reason", deadLetter.reason()).increment();
            log.error("Dead-lettered unprocessable event id={} reason={} table={} type={} kafka={}:{} binlog={}:{} message={}",
                    id, deadLetter.reason(), deadLetter.table(), deadLetter.eventType(),
                    deadLetter.kafkaPartition(), deadLetter.kafkaOffset(),
                    deadLetter.binlogFile(), deadLetter.binlogPosition(), deadLetter.message());
        }
    }

    /** 最近的死信，新的在前。 */
    public List<Entry> recent(int count) {
        List<MapRecord<String, Object, Object>> records = redis.opsForStream()
                .reverseRange(streamKey, Range.unbounded(), Limit.limit().count(count));
        if (records == null) {
            return List.of();
        }
        return records.stream().map(record -> {
            Map<String, String> fields = new LinkedHashMap<>();
            record.getValue().forEach((name, value) -> fields.put(String.valueOf(name), String.valueOf(value)));
            return new Entry(record.getId().getValue(), fields);
        }).toList();
    }

    private Map<String, String> fields(DeadLetter deadLetter) {
        Map<String, String> fields = new LinkedHashMap<>();
        fields.put("recordedAt", Instant.now().toString());
        fields.put("reason", deadLetter.reason());
        put(fields, "message", deadLetter.message());
        put(fields, "table", deadLetter.table());
        put(fields, "eventType", deadLetter.eventType());
        put(fields, "kafkaPartition", deadLetter.kafkaPartition());
        put(fields, "kafkaOffset", deadLetter.kafkaOffset());
        put(fields, "binlogFile", deadLetter.binlogFile());
        put(fields, "binlogPosition", deadLetter.binlogPosition());
        put(fields, "before", deadLetter.before() == null ? null : json(deadLetter.before()));
        put(fields, "after", deadLetter.after() == null ? null : json(deadLetter.after()));
        return fields;
    }

    private static void put(Map<String, String> fields, String name, Object value) {
        if (value != null) {
            String text = value.toString();
            fields.put(name, text.length() > MAX_VALUE_CHARS ? text.substring(0, MAX_VALUE_CHARS) + "...(truncated)" : text);
        }
    }

    private String json(Map<String, Object> data) {
        try {
            return objectMapper.writeValueAsString(data);
        } catch (JsonProcessingException e) {
            return String.valueOf(data);
        }
    }

    public record DeadLetter(String reason, String message, String table, String eventType,
                             Integer kafkaPartition, Long kafkaOffset, String binlogFile, Long binlogPosition,
                             Map<String, Object> before, Map<String, Object> after) {

        public static DeadLetter of(BinlogEvent event, UnprocessableEventException error) {
            return new DeadLetter(error.getReason().name(), error.getMessage(), event.getTableName(),
                    event.getEventType() == null ? null : event.getEventType().name(),
                    event.getKafkaPartition(), event.getKafkaOffset(), event.getBinlogFile(), event.getBinlogPosition(),
                    event.getBeforeData(), event.getAfterData());
        }
    }

    public record Entry(String id, Map<String, String> fields) {
    }
}
