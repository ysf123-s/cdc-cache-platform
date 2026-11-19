package com.example.canalcache.service;

import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.alibaba.otter.canal.protocol.FlatMessage;
import com.example.canalcache.detector.BigKeyDetector;
import com.example.canalcache.diagnostics.TraceCollector;
import com.example.canalcache.model.AutoCacheConfig;
import com.example.canalcache.model.BinlogEvent;
import com.example.canalcache.model.BinlogVersion;
import com.example.canalcache.model.KafkaCanalRecord;
import com.example.canalcache.model.SyncStatus;
import com.example.canalcache.model.UnprocessableEventException;
import com.example.canalcache.parser.BinlogParser;
import com.example.canalcache.parser.FlatMessageBinlogParser;
import com.example.canalcache.service.DeadLetterStore.DeadLetter;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.InterruptException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * MQ 模式下的缓存同步核心服务。
 */
@Slf4j
@Service
public class CanalCacheSyncService {
    private static final int PACKED_EPOCH_BITS = 5;
    private static final int PACKED_FILE_NO_BITS = 16;
    private static final int PACKED_OFFSET_BITS = 32;
    private static final long MAX_PACKED_EPOCH = (1L << PACKED_EPOCH_BITS) - 1;
    private static final long MAX_PACKED_FILE_NO = (1L << PACKED_FILE_NO_BITS) - 1;
    private static final long MAX_PACKED_OFFSET = (1L << PACKED_OFFSET_BITS) - 1;
    private static final int FILE_NO_SHIFT = 32;
    private static final int EPOCH_SHIFT = FILE_NO_SHIFT + PACKED_FILE_NO_BITS;
    private static final Pattern BINLOG_FILE_NO_PATTERN = Pattern.compile(".*\\.(\\d+)$");
    /** 清空、删除、改名后，原表名下的行全部失效；ALTER 等其他 DDL 不影响已缓存的行。 */
    private static final Set<EventType> PURGE_DDL_TYPES = EnumSet.of(EventType.TRUNCATE, EventType.ERASE, EventType.RENAME);
    static final long INITIAL_RETRY_DELAY_MS = 1_000;
    static final long MAX_RETRY_DELAY_MS = 30_000;

    private final CanalConnectorWrapper canalConnector;
    private final BinlogParser binlogParser;
    private final FlatMessageBinlogParser flatMessageBinlogParser;
    private final CacheAnnotationProcessor cacheAnnotationProcessor;
    private final VersionControllerWithLua versionController;
    private final BigKeyDetector bigKeyDetector;
    private final RedisTemplate<String, String> redisTemplate;
    private final ObjectMapper objectMapper;
    private final ObjectMapper binlogMapper;
    private final TraceCollector traceCollector;
    private final DeadLetterStore deadLetterStore;
    private final CacheNamespacePurger namespacePurger;

    private final Counter processedCounter;
    private final Counter failedCounter;
    private final Counter deletedCounter;
    private final Counter updatedCounter;
    private final Counter purgedCounter;
    private final Counter refreshFallbackCounter;
    private final Timer batchProcessingTimer;
    private final Timer invalidationLagTimer;

    private volatile boolean running = false;
    private final AtomicInteger activeConsumers = new AtomicInteger();
    private volatile Instant lastSuccessAt;
    private volatile SyncStatus.ErrorInfo lastError;
    private ExecutorService syncExecutor;
    private final Set<String> warnedFlatMessageTables = ConcurrentHashMap.newKeySet();

    /**
     * 数据源代数。主从切换、重建或 binlog 序列重置后递增，避免新数据源的低编号 binlog
     * 被误判为旧事件。默认 0 时保持旧版 version 编码完全不变。
     */
    @Value("${canal.source-epoch:0}")
    private long sourceEpoch;

    public CanalCacheSyncService(
            CanalConnectorWrapper canalConnector,
            BinlogParser binlogParser,
            FlatMessageBinlogParser flatMessageBinlogParser,
            CacheAnnotationProcessor cacheAnnotationProcessor,
            VersionControllerWithLua versionController,
            BigKeyDetector bigKeyDetector,
            RedisTemplate<String, String> redisTemplate,
            ObjectMapper objectMapper,
            MeterRegistry meterRegistry) {
        this(canalConnector, binlogParser, flatMessageBinlogParser, cacheAnnotationProcessor,
                versionController, bigKeyDetector, redisTemplate, objectMapper, meterRegistry, new TraceCollector());
    }

    public CanalCacheSyncService(
            CanalConnectorWrapper canalConnector,
            BinlogParser binlogParser,
            FlatMessageBinlogParser flatMessageBinlogParser,
            CacheAnnotationProcessor cacheAnnotationProcessor,
            VersionControllerWithLua versionController,
            BigKeyDetector bigKeyDetector,
            RedisTemplate<String, String> redisTemplate,
            ObjectMapper objectMapper,
            MeterRegistry meterRegistry,
            TraceCollector traceCollector) {
        this(canalConnector, binlogParser, flatMessageBinlogParser, cacheAnnotationProcessor,
                versionController, bigKeyDetector, redisTemplate, objectMapper, meterRegistry, traceCollector,
                new DeadLetterStore(redisTemplate, objectMapper, meterRegistry),
                new CacheNamespacePurger(redisTemplate, new CacheBrowseService(redisTemplate)));
    }

    @Autowired
    public CanalCacheSyncService(
            CanalConnectorWrapper canalConnector,
            BinlogParser binlogParser,
            FlatMessageBinlogParser flatMessageBinlogParser,
            CacheAnnotationProcessor cacheAnnotationProcessor,
            VersionControllerWithLua versionController,
            BigKeyDetector bigKeyDetector,
            RedisTemplate<String, String> redisTemplate,
            ObjectMapper objectMapper,
            MeterRegistry meterRegistry,
            TraceCollector traceCollector,
            DeadLetterStore deadLetterStore,
            CacheNamespacePurger namespacePurger) {
        this.canalConnector = canalConnector;
        this.binlogParser = binlogParser;
        this.flatMessageBinlogParser = flatMessageBinlogParser;
        this.cacheAnnotationProcessor = cacheAnnotationProcessor;
        this.versionController = versionController;
        this.bigKeyDetector = bigKeyDetector;
        this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;
        this.traceCollector = traceCollector;
        this.deadLetterStore = deadLetterStore;
        this.namespacePurger = namespacePurger;
        // 当前缓存实体使用下划线列名；DATETIME 与 JDBC 的 Asia/Shanghai 配置保持一致。
        var dateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        dateFormat.setTimeZone(java.util.TimeZone.getTimeZone("Asia/Shanghai"));
        this.binlogMapper = objectMapper.copy().setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
                .setDateFormat(dateFormat);

        this.processedCounter = Counter.builder("canal.events.processed")
                .description("Total binlog events processed successfully")
                .register(meterRegistry);
        this.failedCounter = Counter.builder("canal.events.failed")
                .description("Total binlog event batches that failed and rolled back")
                .register(meterRegistry);
        this.deletedCounter = Counter.builder("canal.cache.deleted")
                .description("Total cache keys deleted")
                .register(meterRegistry);
        this.updatedCounter = Counter.builder("canal.cache.updated")
                .description("Total cache keys updated")
                .register(meterRegistry);
        this.purgedCounter = Counter.builder("canal.cache.purged")
                .description("Total cache keys removed by table-level DDL purges")
                .register(meterRegistry);
        this.refreshFallbackCounter = Counter.builder("canal.cache.refresh.fallback")
                .description("Refreshes that fell back to delete because row data could not be converted")
                .register(meterRegistry);
        this.batchProcessingTimer = Timer.builder("canal.batch.processing.time")
                .description("Time to process a batch of binlog events")
                .register(meterRegistry);
        this.invalidationLagTimer = Timer.builder("canal.cache.invalidation.lag")
                .description("Source commit time to cache handling completion; assumes synchronized clocks")
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(meterRegistry);
    }

    @PostConstruct
    public void start() {
        running = true;
        int consumerThreads = Math.max(1, canalConnector.getConsumerThreadCount());
        AtomicInteger consumerNo = new AtomicInteger(1);
        syncExecutor = new ThreadPoolExecutor(
                consumerThreads,
                consumerThreads,
                0L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(1000),
                r -> new Thread(r, "canal-cache-partition-consumer-" + consumerNo.getAndIncrement()),
                new ThreadPoolExecutor.AbortPolicy()
        );

        for (int index = 0; index < consumerThreads; index++) {
            syncExecutor.submit(this::syncLoop);
        }

        log.info("CanalCacheSyncService MQ version started, flatMessageMode={}, consumerThreads={}",
                canalConnector.isFlatMessageMode(), consumerThreads);
    }

    /**
     * 单个 Kafka consumer 线程的同步循环。
     */
    private void syncLoop() {
        CanalConnectorWrapper.PartitionConsumer consumer = null;
        try {
            // 创建一个consumer，并长期复用
            consumer = canalConnector.createPartitionConsumer();
            activeConsumers.incrementAndGet();
            try {
                doSyncLoop(consumer);
            } finally {
                activeConsumers.decrementAndGet();
            }
        } catch (Exception e) {
            if (running) {
                recordFailure(e);
                log.error("Kafka consumer stopped unexpectedly", e);
            }
        } finally {
            if (consumer != null) {
                consumer.close();
            }
        }
    }

    private void doSyncLoop(CanalConnectorWrapper.PartitionConsumer consumer) {
        // 连续失败次数决定重试间隔；任一批次成功后归零。
        int consecutiveFailures = 0;
        while (running && !Thread.currentThread().isInterrupted()) {
            try {
                if (!processBatch(consumer)) {
                    Thread.sleep(10);
                }
                consecutiveFailures = 0;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (InterruptException e) {
                if (!running || Thread.currentThread().isInterrupted()) {
                    log.info("MQ sync loop interrupted during shutdown");
                    Thread.currentThread().interrupt();
                    break;
                }
                log.warn("MQ sync loop interrupted unexpectedly", e);
                failedCounter.increment();
                recordFailure(e);
                consumer.rollback();
                sleepBeforeRetry(++consecutiveFailures);
            } catch (Exception e) {
                log.error("Error in MQ sync loop", e);
                failedCounter.increment();
                recordFailure(e);
                consumer.rollback();
                sleepBeforeRetry(++consecutiveFailures);
            }
        }
    }

    /**
     * 单个 partition 内顺序处理当前批次。并行度来自多个 partition consumer 线程。
     *
     * <p>只有能确定无法处理的事件会被收集为死信并跳过；Redis 等外部故障照常抛出，由外层整批回滚重试。</p>
     */
    private List<DeadLetter> processEvents(List<BinlogEvent> events) {
        List<DeadLetter> deadLetters = new ArrayList<>();
        for (BinlogEvent event : events) {
            try {
                if (event.isDdl()) {
                    handleDdl(event);
                } else {
                    handleEvent(event);
                }
            } catch (UnprocessableEventException e) {
                deadLetters.add(DeadLetter.of(event, e));
            }
        }
        return deadLetters;
    }

    /**
     * 只有解析和处理全部成功才确认本批；被正常过滤的消息也需要确认。
     * 无法处理的消息在确认前写入死信，整批回滚时不会写入，避免重试产生重复死信。
     * 返回 false 表示没有拉到 Kafka 消息。
     */
    boolean processBatch(CanalConnectorWrapper.PartitionConsumer consumer) {
        List<BinlogEvent> events = new ArrayList<>();
        List<DeadLetter> deadLetters = new ArrayList<>();
        List<String> eventCodes = new ArrayList<>();
        long fetchCostNs = 0L;
        long receiveEpochMs = System.currentTimeMillis();

        if (canalConnector.isFlatMessageMode()) {
            long tFetchStart = System.nanoTime();
            List<FlatMessage> flatMessages = consumer.getFlatMessagesWithoutAck();
            fetchCostNs = System.nanoTime() - tFetchStart;
            receiveEpochMs = System.currentTimeMillis();
            if (flatMessages.isEmpty()) {
                return false;
            }
            long tParseStart = System.nanoTime();
            List<BinlogEvent> parsed = parseFlatMessages(flatMessages, deadLetters);
            long parseCostNs = System.nanoTime() - tParseStart;
            for (BinlogEvent event : parsed) {
                event.setFetchCostNs(fetchCostNs);
                event.setParseCostNs(parseCostNs);
                event.setReceiveEpochMs(receiveEpochMs);
            }
            events.addAll(parsed);
        } else {
            long tFetchStart = System.nanoTime();
            List<KafkaCanalRecord> records = consumer.getRecordsWithoutAck();
            fetchCostNs = System.nanoTime() - tFetchStart;
            receiveEpochMs = System.currentTimeMillis();
            if (records.isEmpty()) {
                return false;
            }
            for (KafkaCanalRecord record : records) {
                long tParseStart = System.nanoTime();
                List<BinlogEvent> parsed = parseRecord(record, deadLetters);
                long parseCostNs = System.nanoTime() - tParseStart;
                for (BinlogEvent event : parsed) {
                    event.setKafkaPartition(record.partition());
                    event.setKafkaOffset(record.offset());
                    event.setFetchCostNs(fetchCostNs);
                    event.setParseCostNs(parseCostNs);
                    event.setReceiveEpochMs(receiveEpochMs);
                }
                events.addAll(parsed);
            }
        }

        deadLetters.addAll(batchProcessingTimer.record(() -> processEvents(events)));

        for (BinlogEvent event : events) {
            String code = extractEventCode(event);
            if (code != null) {
                eventCodes.add(code);
            }
        }

        deadLetterStore.recordAll(deadLetters);

        long tAckStart = System.nanoTime();
        consumer.ack();
        long ackCostNs = System.nanoTime() - tAckStart;

        traceCollector.recordConsumerAck(eventCodes, ackCostNs);
        recordBatchSuccess();
        return true;
    }

    /** 逐个 Entry 解析；损坏的 Entry 进入死信，同一条 Kafka 消息中的其他 Entry 照常处理。 */
    private List<BinlogEvent> parseRecord(KafkaCanalRecord record, List<DeadLetter> deadLetters) {
        List<BinlogEvent> events = new ArrayList<>();
        List<Entry> entries = record.message().getEntries();
        for (int index = 0; index < entries.size(); index++) {
            Entry entry = entries.get(index);
            try {
                events.addAll(binlogParser.parseEntry(entry, index));
            } catch (UnprocessableEventException e) {
                deadLetters.add(new DeadLetter(e.getReason().name(), e.getMessage(), entry.getHeader().getTableName(),
                        null, record.partition(), record.offset(), entry.getHeader().getLogfileName(),
                        entry.getHeader().getLogfileOffset(), null, null));
            }
        }
        return events;
    }

    private List<BinlogEvent> parseFlatMessages(List<FlatMessage> flatMessages, List<DeadLetter> deadLetters) {
        List<BinlogEvent> events = new ArrayList<>();
        for (FlatMessage message : flatMessages) {
            try {
                events.addAll(flatMessageBinlogParser.parseMessage(message));
            } catch (UnprocessableEventException e) {
                deadLetters.add(new DeadLetter(e.getReason().name(), e.getMessage(), message.getTable(),
                        message.getType(), null, null, null, null, null, null));
            }
        }
        return events;
    }

    public SyncStatus getStatus() {
        return new SyncStatus(running, Math.max(1, canalConnector.getConsumerThreadCount()),
                activeConsumers.get(), lastSuccessAt, lastError);
    }

    private synchronized void recordBatchSuccess() {
        // ack 失败不能更新成功时间；空轮询不更新，正常过滤并确认的批次会更新。
        lastSuccessAt = Instant.now();
    }

    private synchronized void recordFailure(Exception error) {
        lastError = new SyncStatus.ErrorInfo(Instant.now(), Thread.currentThread().getName(),
                error.getClass().getSimpleName(), error.getMessage());
    }

    /**
     * 处理一条行级变更事件。public 供集成测试和补偿场景直接调用。
     */
    public void handleEvent(BinlogEvent event) {
        String tableName = event.getTableName();

        AutoCacheConfig config = cacheAnnotationProcessor.getConfig(tableName);
        if (config == null) {
            return;
        }

        Map<String, Object> targetData = resolveTargetData(event);
        if (targetData == null) {
            // 该事件类型无可用数据
            log.warn("No data available for event type: {}", event.getEventType());
            return;
        }

        // 拿到配置的主键，根据主键得到数据中的具体的数据。
        // binlog_row_image=MINIMAL 时 UPDATE 后镜像只含变更列，主键未变时从前镜像取；
        // 此时行数据不完整，不能用于刷新缓存，只能删除。
        Object idValue = targetData.get(config.getIdField());
        boolean partialImage = idValue == null;
        if (partialImage && event.getBeforeData() != null) {
            idValue = event.getBeforeData().get(config.getIdField());
        }
        if (idValue == null) {
            throw new UnprocessableEventException(UnprocessableEventException.Reason.MISSING_PRIMARY_KEY,
                    "Missing primary key " + config.getIdField() + " in binlog event for table " + tableName);
        }

        String cacheKey = config.buildCacheKeySimple(idValue);
        executeStrategy(config, event, cacheKey, partialImage ? null : targetData);
        invalidateIndexKeys(config, event);
        processedCounter.increment();
        if (event.getEventTimestamp() > 0) {
            invalidationLagTimer.record(Duration.ofMillis(
                    Math.max(0, System.currentTimeMillis() - event.getEventTimestamp())));
        }
    }

    /**
     * 二级索引缓存存的是“列值 → 主键列表”。一行的列值从 A 改成 B，A 和 B 两个列表都变了，
     * 所以用修改前、修改后的列值各删一次；列值和主键都没变的 UPDATE 不影响列表，跳过。
     * 索引缓存只由查询回填（version=0），事件从不刷新它，统一用 DEL，删除与顺序无关、可重复。
     */
    private void invalidateIndexKeys(AutoCacheConfig config, BinlogEvent event) {
        if (config.getIndexKeys().isEmpty()) {
            return;
        }
        Map<String, Object> before = event.getBeforeData();
        Map<String, Object> after = event.getEventType() == EventType.DELETE ? null : event.getAfterData();
        String idField = config.getIdField();
        boolean primaryKeyChanged = before != null && after != null && after.containsKey(idField)
                && !Objects.equals(before.get(idField), after.get(idField));
        Set<String> keys = new LinkedHashSet<>();
        for (AutoCacheConfig.IndexKey index : config.getIndexKeys()) {
            Object oldValue = before == null ? null : before.get(index.column());
            // MINIMAL 行镜像的后镜像不含未修改的列，此时只能按前镜像删除。
            boolean afterHasColumn = after != null && after.containsKey(index.column());
            Object newValue = afterHasColumn ? after.get(index.column()) : null;
            if (before != null && afterHasColumn && !primaryKeyChanged && Objects.equals(oldValue, newValue)) {
                continue;
            }
            if (oldValue != null) {
                keys.add(index.buildKey(oldValue));
            }
            if (newValue != null) {
                keys.add(index.buildKey(newValue));
            }
        }
        if (!keys.isEmpty()) {
            Long deleted = redisTemplate.delete(List.copyOf(keys));
            deletedCounter.increment(deleted == null ? 0 : deleted);
        }
    }

    /**
     * 已注册表被清空、删除或改名时，没有逐行事件，整个缓存分组都要失效。
     * 只做删除，所以与其他 partition 上的行事件谁先处理都不影响结果。
     */
    private void handleDdl(BinlogEvent event) {
        if (!PURGE_DDL_TYPES.contains(event.getEventType())) {
            return;
        }
        AutoCacheConfig config = cacheAnnotationProcessor.getConfig(event.getTableName());
        if (config == null) {
            return;
        }
        long purged = namespacePurger.purge(event.getTableName(), config);
        purgedCounter.increment(purged);
        log.info("Purged {} cache keys of table {} after {}: {}", purged, event.getTableName(),
                event.getEventType(), event.getDdlSql());
    }

    /**
     * INSERT/UPDATE use afterData, DELETE uses beforeData.
     */
    private Map<String, Object> resolveTargetData(BinlogEvent event) {
        EventType eventType = event.getEventType();
        if (eventType == EventType.INSERT || eventType == EventType.UPDATE) {
            return event.getAfterData();
        }
        if (eventType == EventType.DELETE) {
            return event.getBeforeData();
        }
        return null;
    }

    /**
     * 根据策略决定缓存怎么处理。
     */
    private void executeStrategy(AutoCacheConfig config, BinlogEvent event,
                                 String cacheKey, Map<String, Object> data) {
        boolean useVersionControl = shouldUseVersionControl(config);
        BinlogVersion version = useVersionControl
                ? buildVersion(sourceEpoch, event.getBinlogFile(), event.getBinlogPosition()) : null;

        // 主键变更相当于旧行消失；新 key 仍按原有更新策略处理。
        if (event.getEventType() == EventType.UPDATE && event.getBeforeData() != null) {
            Object oldId = event.getBeforeData().get(config.getIdField());
            if (oldId != null) {
                String oldKey = config.buildCacheKeySimple(oldId);
                if (!oldKey.equals(cacheKey)) {
                    doDelete(config, oldKey, version, useVersionControl, null);
                }
            }
        }

        // 新增也删除缓存，清理此前查询不到用户时留下的 null 标记，不做主动预热。
        // data 为 null 表示行镜像不完整（见 handleEvent），任何策略都只能删除。
        if (event.getEventType() == EventType.INSERT || event.getEventType() == EventType.DELETE || data == null) {
            doDelete(config, cacheKey, version, useVersionControl, event);
            return;
        }

        switch (config.getStrategy()) {
            case DELETE_MODE:
                doDelete(config, cacheKey, version, useVersionControl, event);
                break;
            case REFRESH_MODE:
                doRefresh(config, cacheKey, data, version, useVersionControl, event);
                break;
            case HYBRID_MODE:
                doHybrid(config, cacheKey, data, version, useVersionControl, event);
                break;
            default:
                break;
        }
    }

    private void doDelete(AutoCacheConfig config, String cacheKey, BinlogVersion version, boolean enableVersionControl) {
        doDelete(config, cacheKey, version, enableVersionControl, null);
    }

    /**
     * 删除缓存。
     * 这里统一使用 DEL，建议 Redis 侧开启 lazyfree-lazy-user-del=yes。
     */
    private void doDelete(AutoCacheConfig config, String cacheKey, BinlogVersion version,
                          boolean enableVersionControl, BinlogEvent event) {
        // 删除也使用版本比较。
        if (enableVersionControl) {
            long tLuaStart = System.nanoTime();
            boolean deleted = versionController.deleteIfNewerVersion(cacheKey, version);
            long luaCostNs = System.nanoTime() - tLuaStart;
            long luaEndEpochMs = System.currentTimeMillis();
            if (deleted) {
                deletedCounter.increment();
                log.debug("Deleted cache with version {}: {}", version, cacheKey);
            }
            recordLuaTrace(event, version, luaCostNs, luaEndEpochMs);
            return;
        }

        long tLuaStart = System.nanoTime();
        Boolean deleted = redisTemplate.delete(cacheKey);
        long luaCostNs = System.nanoTime() - tLuaStart;
        long luaEndEpochMs = System.currentTimeMillis();
        if (Boolean.TRUE.equals(deleted)) {
            deletedCounter.increment();
            log.debug("Deleted cache: {}", cacheKey);
        }
        recordLuaTrace(event, version, luaCostNs, luaEndEpochMs);
    }

    private void doRefresh(AutoCacheConfig config, String cacheKey, Map<String, Object> data,
                           BinlogVersion version, boolean enableVersionControl) {
        doRefresh(config, cacheKey, data, version, enableVersionControl, null);
    }

    /**
     * 刷新缓存内容。
     * 版本控制开启时用 Lua 脚本原子比较 binlog 版本位点后写入 Hash；
     * 关闭时也只刷新已有 Hash（data + version=0）。
     * 行数据无法转换为缓存实体时降级为删除：删除只依赖主键，不会因数据格式失败。
     */
    private void doRefresh(AutoCacheConfig config, String cacheKey, Map<String, Object> data,
                           BinlogVersion version, boolean enableVersionControl, BinlogEvent event) {
        String json = toCacheJson(config, cacheKey, data);
        if (json == null) {
            refreshFallbackCounter.increment();
            doDelete(config, cacheKey, version, enableVersionControl, event);
            return;
        }

        if (enableVersionControl) {
            long tLuaStart = System.nanoTime();
            boolean updated = versionController.updateIfNewerVersion(cacheKey, json, version, config.getExpireSeconds());
            long luaCostNs = System.nanoTime() - tLuaStart;
            long luaEndEpochMs = System.currentTimeMillis();
            if (updated) {
                updatedCounter.increment();
                log.debug("Refreshed cache with version {}: {}", version, cacheKey);
            }
            recordLuaTrace(event, version, luaCostNs, luaEndEpochMs);
            return;
        }

        long tLuaStart = System.nanoTime();
        boolean updated = versionController.updateIfPresent(cacheKey, json, config.getExpireSeconds());
        long luaCostNs = System.nanoTime() - tLuaStart;
        long luaEndEpochMs = System.currentTimeMillis();
        if (updated) {
            updatedCounter.increment();
            log.debug("Refreshed cache: {}", cacheKey);
        }
        recordLuaTrace(event, version, luaCostNs, luaEndEpochMs);
    }

    private void doHybrid(AutoCacheConfig config, String cacheKey, Map<String, Object> data,
                          BinlogVersion version, boolean enableVersionControl) {
        doHybrid(config, cacheKey, data, version, enableVersionControl, null);
    }

    /**
     * 混合策略：
     * 大 key → 删除；
     * 缓存存在 → 更新（不存在的冷数据不写入）；
     * 缓存不存在 → 什么都不做。
     */
    private void doHybrid(AutoCacheConfig config, String cacheKey, Map<String, Object> data,
                          BinlogVersion version, boolean enableVersionControl, BinlogEvent event) {
        if (bigKeyDetector.isBigKey(data, config.getMaxSizeBytes())) {
            doDelete(config, cacheKey, version, enableVersionControl, event);
            log.info("Big key detected, deleted instead of refreshed: {}", cacheKey);
            return;
        }

        doRefresh(config, cacheKey, data, version, enableVersionControl, event);
    }

    private String extractEventCode(BinlogEvent event) {
        if (event == null) {
            return null;
        }
        // 压测链路只追踪业务行更新（UPDATE）事件，避免压测清理阶段的 DELETE 事件错误覆盖末尾样本的 Trace 记录
        if (event.getEventType() == EventType.UPDATE && event.getAfterData() != null && event.getAfterData().get("code") != null) {
            return String.valueOf(event.getAfterData().get("code"));
        }
        return null;
    }

    private void recordLuaTrace(BinlogEvent event, BinlogVersion version, long luaCostNs, long luaEndEpochMs) {
        if (event == null) {
            return;
        }
        String code = extractEventCode(event);
        if (code == null) {
            return;
        }
        traceCollector.recordConsumerLua(
                code,
                version != null ? version.encodedVersion() : null,
                event.getBinlogFile(),
                event.getBinlogPosition(),
                event.getEventTimestamp(),
                event.getKafkaPartition(),
                event.getKafkaOffset(),
                event.getFetchCostNs(),
                event.getParseCostNs(),
                event.getReceiveEpochMs(),
                luaCostNs,
                luaEndEpochMs
        );
    }

    /**
     * 只有在非 FlatMessage 模式下，版本控制才有真实意义。
     * FlatMessage 模式下 binlogPosition 固定为 0，开启版本控制会导致所有事件被 Lua 拒绝。
     * warn 每张表只打一次，避免高频日志刷屏。
     */
    private boolean shouldUseVersionControl(AutoCacheConfig config) {
        if (canalConnector.isFlatMessageMode() && config.isEnableVersionControl()) {
            String tableName = config.getTable() == null ? "<unknown>" : config.getTable();
            if (warnedFlatMessageTables.add(tableName)) {
                // 打印这个表示模式不匹配或者没开版本控制，且只打印一次
                log.warn("table={} 配置了 enableVersionControl=true，但当前为 FlatMessage 模式，" +
                        "FlatMessage 无法提供真实 binlogPosition，版本控制已自动关闭。" +
                        "如需版本控制请切换 flat-message: false", tableName);
            }
        }
        return !canalConnector.isFlatMessageMode() && config.isEnableVersionControl();
    }

    /**
     * 把一行数据转成 JSON，作为 Redis 缓存值；无法转换时返回 null。
     * 先还原查询侧的实体类型，再使用同一个应用 mapper 输出缓存格式。
     */
    private String toCacheJson(AutoCacheConfig config, String cacheKey, Map<String, Object> data) {
        try {
            return objectMapper.writeValueAsString(binlogMapper.convertValue(data, config.getEntityClass()));
        } catch (IllegalArgumentException | JsonProcessingException e) {
            log.warn("Cannot convert row data for {}, falling back to delete", cacheKey, e);
            return null;
        }
    }

    /** 连续失败时按 1s、2s、4s… 递增等待，最长 30s，避免外部依赖长时间故障时空转刷日志。 */
    static long retryDelayMs(int consecutiveFailures) {
        int exponent = Math.min(Math.max(consecutiveFailures, 1) - 1, 5);
        return Math.min(MAX_RETRY_DELAY_MS, INITIAL_RETRY_DELAY_MS << exponent);
    }

    private void sleepBeforeRetry(int consecutiveFailures) {
        try {
            Thread.sleep(retryDelayMs(consecutiveFailures));
        } catch (InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
        }
    }

    @PreDestroy
    public void stop() {
        running = false;
        if (syncExecutor != null) {
            syncExecutor.shutdown();
            try {
                if (!syncExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    syncExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                syncExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        log.info("CanalCacheSyncService MQ version stopped");
    }

    /**
     * 解析 source epoch、binlog 文件名和偏移量，生成可比较的版本位点。
     *
     * <p>按 5 bit epoch + 16 bit fileNo + 32 bit offset 打包为 encodedVersion，
     * Lua 直接比较这个单值。</p>
     *
     * <p>source epoch 在主从切换、重建或 RESET MASTER 后由部署侧递增。</p>
     */
    private BinlogVersion buildVersion(long epoch, String binlogFile, long offset) {
        long safeEpoch = checkInRange(epoch, 0, MAX_PACKED_EPOCH, "sourceEpoch");
        long safeFileNo = checkInRange(parseBinlogFileNo(binlogFile), 0, MAX_PACKED_FILE_NO, "binlog fileNo");
        long safeOffset = checkInRange(offset, 0, MAX_PACKED_OFFSET, "binlog offset");

        //  <<意思是左移补零，| 按位合并，
        // long encoded 在内存里是 64 位二进制；
        // println(encoded) 打印出来是十进制数字
        // 后续是因为Redis存不了long，所以要转成String
        long encodedVersion = (safeEpoch << EPOCH_SHIFT)
                | (safeFileNo << FILE_NO_SHIFT)
                | (safeOffset & MAX_PACKED_OFFSET);
        return new BinlogVersion(safeEpoch, safeFileNo, safeOffset, encodedVersion);
    }

    private long parseBinlogFileNo(String binlogFile) {
        if (binlogFile == null) {
            return 0;
        }
        Matcher matcher = BINLOG_FILE_NO_PATTERN.matcher(binlogFile);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid binlog file name: " + binlogFile);
        }
        // string转Long
        return Long.parseLong(matcher.group(1));
    }

    private long checkInRange(long value, long minValue, long maxValue, String name) {
        if (value < minValue || value > maxValue) {
            throw new IllegalArgumentException(
                    name + " out of range: value=" + value + ", expected=[" + minValue + "," + maxValue + "]"
            );
        }
        return value;
    }
}
