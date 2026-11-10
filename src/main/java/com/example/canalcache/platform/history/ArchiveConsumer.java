package com.example.canalcache.platform.history;

import com.example.canalcache.model.KafkaCanalRecord;
import com.example.canalcache.model.SyncStatus;
import com.example.canalcache.service.CanalConnectorWrapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.InterruptException;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class ArchiveConsumer implements AutoCloseable {
    private final CanalConnectorWrapper connector;
    private final ArchiveService archive;
    private final String groupId;
    private final AtomicLong inserted = new AtomicLong();
    private final AtomicLong duplicates = new AtomicLong();
    private final Map<String, String> acknowledgedOffsets = new ConcurrentHashMap<>();
    private volatile boolean running;
    private volatile boolean active;
    private volatile Instant lastSuccessAt;
    private volatile SyncStatus.ErrorInfo lastError;
    private Thread thread;

    public ArchiveConsumer(CanalConnectorWrapper connector, ArchiveService archive, String groupId) {
        this.connector = connector;
        this.archive = archive;
        this.groupId = groupId;
    }

    public void start() {
        running = true;
        thread = new Thread(this::run, "platform-archive-consumer");
        thread.start();
    }

    private void run() {
        try (CanalConnectorWrapper.PartitionConsumer consumer = connector.createArchiveConsumer(groupId)) {
            active = true;
            while (running && !Thread.currentThread().isInterrupted()) {
                try {
                    if (!processBatch(consumer)) { Thread.sleep(10); }
                } catch (InterruptedException | InterruptException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (RuntimeException e) {
                    recordError(e);
                    consumer.rollback();
                    try { Thread.sleep(1000); }
                    catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); break; }
                }
            }
        } catch (RuntimeException e) {
            if (running) { recordError(e); }
        } finally { active = false; }
    }

    public boolean processBatch(CanalConnectorWrapper.PartitionConsumer consumer) {
        List<KafkaCanalRecord> records = consumer.getRecordsWithoutAck();
        if (records.isEmpty()) { return false; }
        ArchiveService.Result result = archive.archiveBatch(records);
        // The whole poll's database transaction has committed before advancing Kafka progress.
        consumer.ack();
        inserted.addAndGet(result.inserted());
        duplicates.addAndGet(result.duplicates());
        for (KafkaCanalRecord record : records) {
            acknowledgedOffsets.put(record.topic() + ":" + record.partition(), Long.toString(record.offset() + 1));
        }
        lastSuccessAt = Instant.now();
        return true;
    }

    private void recordError(RuntimeException error) {
        lastError = new SyncStatus.ErrorInfo(Instant.now(), Thread.currentThread().getName(),
                error.getClass().getSimpleName(), error.getMessage());
        log.error("Archive consumer failed; Kafka progress is not advanced", error);
    }

    public Status status() {
        return new Status(running, active, groupId, lastSuccessAt, lastError, inserted.get(), duplicates.get(),
                Map.copyOf(acknowledgedOffsets));
    }

    @Override
    public void close() {
        running = false;
        if (thread != null) {
            thread.interrupt();
            try { thread.join(5000); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
    }

    public record Status(boolean serviceStarted, boolean activeConsumer, String groupId, Instant lastSuccessAt,
                         SyncStatus.ErrorInfo lastError, long acknowledgedNewRows, long acknowledgedDuplicateRows,
                         Map<String, String> acknowledgedNextOffsets) { }
}
