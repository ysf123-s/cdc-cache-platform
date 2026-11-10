package com.example.canalcache.platform.replay;

import com.example.canalcache.platform.PlatformProperties;
import com.example.canalcache.platform.history.*;
import com.example.canalcache.platform.subscription.*;
import org.springframework.dao.DataAccessException;
import org.springframework.scheduling.annotation.Scheduled;
import java.time.Instant;
import java.util.*;

public class ReplayService {
    private final PlatformProperties properties;
    private final ArchiveStore store;
    public ReplayService(PlatformProperties properties, ArchiveStore store) { this.properties = properties; this.store = store; }

    public ReplayJob createReplayJob(ReplayJob.Request req) {
        if (req == null || req.requestKey() == null || req.requestKey().isBlank() || req.requestKey().length() > 128) {
            throw new IllegalArgumentException("requestKey must contain 1-128 characters");
        }
        if (req.subscriptionId() == null || req.subscriptionId().isBlank()) throw new IllegalArgumentException("subscriptionId is required");
        if (req.eventIds() == null || req.eventIds().isEmpty() || req.eventIds().size() > 100) {
            throw new IllegalArgumentException("Replay is limited to at most 100 events per job; at least one is required");
        }
        if (req.eventIds().stream().anyMatch(id -> id == null || !id.matches("[a-f0-9]{64}"))) {
            throw new IllegalArgumentException("Every eventId must be a lowercase SHA-256 value");
        }
        List<String> ids = req.eventIds().stream().distinct().sorted().toList();
        ReplayJob existing = store.findReplayJobByRequestKey(req.requestKey());
        if (existing != null) {
            return reuse(req, ids, existing);
        }
        EventSubscription sub = store.findSubscription(req.subscriptionId());
        if (sub == null) throw new NoSuchElementException("Subscription not found");
        if (!sub.enabled()) throw new IllegalArgumentException("Cannot replay against a disabled subscription");
        PlatformProperties.TargetConfig target = properties.getTargets().get(sub.targetName());
        if (target == null) throw new IllegalArgumentException("Target alias is no longer registered: " + sub.targetName());
        String jobId = EventIdentity.hash("replay:" + UUID.randomUUID());
        ReplayJob job = new ReplayJob(jobId, req.requestKey(), properties.getSourceId(), properties.getDatabase(), sub.table(),
                sub.subscriptionId(), "PENDING", ids.size(), 0, 0, 0, ids, store.encode(sub), target.getUrl(), null,
                Instant.now().toString(), Instant.now().toString());
        if (!store.saveReplayJob(job)) return reuse(req, ids, store.findReplayJobByRequestKey(req.requestKey()));
        resume(jobId);
        return store.findReplayJob(jobId);
    }

    private ReplayJob reuse(ReplayJob.Request req, List<String> ids, ReplayJob existing) {
        if (!existing.subscriptionId().equals(req.subscriptionId()) || !existing.eventIds().equals(ids)) {
            throw new IllegalStateException("requestKey is already used by a different replay request");
        }
        resume(existing.jobId());
        return store.findReplayJob(existing.jobId());
    }

    @Scheduled(fixedDelayString = "${platform.replay-delay-ms:500}")
    public void resumePending() { for (String id : store.pendingReplayIds()) resume(id); }

    private void resume(String id) {
        try { processJob(id); }
        catch (DataAccessException e) { store.recordReplayFailure(id, e.getClass().getSimpleName() + ": " + e.getMessage(), false); }
        catch (IllegalStateException | IllegalArgumentException e) { store.recordReplayFailure(id, e.getMessage(), true); }
    }

    /** Bounded to 100 rows: the row lock, tasks and final counts commit together, or all roll back. */
    public ReplayJob processJob(String id) {
        return store.withReplayLock(id, job -> {
            if (!Set.of("PENDING", "RUNNING").contains(job.status())) return job;
            if (job.targetUrl() == null) throw new IllegalStateException("Legacy replay has no frozen target; create a new request");
            EventSubscription sub = store.decode(job.subscriptionSnapshot(), EventSubscription.class);
            int matched = 0, skipped = 0;
            for (String eventId : job.eventIds()) {
                var event = store.findEvent(eventId, properties);
                if (event == null || !job.sourceId().equals(properties.getSourceId())
                        || !job.database().equals(properties.getDatabase()) || !SubscriptionMatcher.matches(event, sub)) {
                    skipped++;
                    continue;
                }
                String deliveryId = EventIdentity.hash("delivery:" + eventId + ":" + sub.subscriptionId() + ":" + job.jobId());
                store.createDeliveryTask(new DeliveryTask(0, deliveryId, eventId, sub.subscriptionId(), "REPLAY", job.jobId(),
                        sub.targetName(), job.targetUrl(), "PENDING", 0, properties.getMaxAttempts(), null, null,
                        Instant.now().toString(), null, store.encode(event), job.subscriptionSnapshot(),
                        Instant.now().toString(), Instant.now().toString()));
                matched++;
            }
            store.updateReplayJobProgress(id, "COMPLETED", matched, skipped, 0);
            return store.findReplayJob(id);
        });
    }

    public ReplayJob getJob(String id) {
        ReplayJob job = store.findReplayJob(id);
        if (job == null) throw new NoSuchElementException("Replay job not found: " + id);
        return job;
    }
}
