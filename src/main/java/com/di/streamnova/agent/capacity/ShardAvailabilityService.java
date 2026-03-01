package com.di.streamnova.agent.capacity;

import com.di.streamnova.config.PipelineConfigService;
import com.di.streamnova.config.PipelineConfigSource;
import com.di.streamnova.guardrail.GuardrailProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.annotation.PostConstruct;

/**
 * Tracks shard/run usage: reserve when a load starts, release when it finishes.
 * Supports exclusive mode (reserve full pool) or shared mode (reserve one run slot, capped by max-concurrent-runs).
 * Max shards: when streamnova.capacity.max-shards &gt; 0 use it; otherwise derived from pipeline_config.yml maximumPoolSize.
 * <p>
 * Race safety: CAS loops on atomics prevent over-allocation; reserve is idempotent per runId (same runId
 * reserved twice returns true without double-counting); release is idempotent (multiple release(runId) safe).
 * <p>
 * When max-concurrent-execute-requests &gt; 0, limits how many execute attempts can be in progress (handles request floods in shared-pool mode).
 */
@Slf4j
@Service
public class ShardAvailabilityService {

    @Value("${streamnova.capacity.max-shards}")
    private int maxShardsOverride;

    @Value("${streamnova.capacity.max-concurrent-execute-requests:0}")
    private int maxConcurrentExecuteRequests;

    private volatile Semaphore executeSemaphore;

    private final PipelineConfigService pipelineConfigService;
    private final GuardrailProperties guardrailProperties;
    private final AtomicInteger shardsInUse = new AtomicInteger(0);
    private final AtomicInteger runSlotsInUse = new AtomicInteger(0);
    private final ConcurrentHashMap<String, Integer> runIdToShards = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Boolean> runIdToRunSlot = new ConcurrentHashMap<>();
    /** Per-request rejection reason when exclusive is rejected due to shared runs (cleared after read). */
    private static final ThreadLocal<String> exclusiveRejectionReason = new ThreadLocal<>();
    /** Per-request rejection reason when max-concurrent-execute-requests is reached (cleared after read). */
    private static final ThreadLocal<String> executeLimitRejectionReason = new ThreadLocal<>();

    public ShardAvailabilityService(PipelineConfigService pipelineConfigService,
                                    GuardrailProperties guardrailProperties) {
        this.pipelineConfigService = pipelineConfigService;
        this.guardrailProperties = guardrailProperties != null ? guardrailProperties : new GuardrailProperties();
    }

    @PostConstruct
    void initExecuteSemaphore() {
        if (maxConcurrentExecuteRequests > 0) {
            executeSemaphore = new Semaphore(maxConcurrentExecuteRequests);
            log.info("[CAPACITY] Max concurrent execute requests: {}", maxConcurrentExecuteRequests);
        }
    }

    /** Max shards: override from application.properties if &gt; 0; else from pipeline default source pool size. */
    private int getMaxShardsLimit() {
        if (maxShardsOverride > 0) return maxShardsOverride;
        PipelineConfigSource src = pipelineConfigService.getEffectiveSourceConfig(null);
        if (src == null) return 0;
        int pool = src.getMaximumPoolSize() > 0 ? src.getMaximumPoolSize() : src.getFallbackPoolSize();
        return Math.max(0, pool);
    }

    /**
     * Tries to reserve for a run (legacy: same as shared, requiredShards used when exclusive is false is not applied).
     * Prefer {@link #tryReserve(int, String, boolean)} to pass exclusive.
     */
    public boolean tryReserve(int requiredShards, String runId) {
        return tryReserve(requiredShards, runId, false);
    }

    /**
     * Tries to reserve for a run. Exclusive: reserve full pool (no other run can start). Shared: reserve one run slot (capped by max-concurrent-runs).
     * Idempotent per runId: if this runId is already reserved (exclusive or shared), returns true without double-counting (avoids slot leak on retry/race).
     *
     * @param requiredShards shards needed when exclusive (ignored when exclusive=false)
     * @param runId         unique id for this run (used to release later)
     * @param exclusive     when true, reserve full pool; when false, reserve one run slot (subject to max-concurrent-runs)
     * @return true if reserved (or no limit), false if not enough capacity
     */
    public boolean tryReserve(int requiredShards, String runId, boolean exclusive) {
        if (runId == null || runId.isBlank()) return false;
        exclusiveRejectionReason.remove();
        executeLimitRejectionReason.remove();

        // Idempotent: same runId already reserved (retry or duplicate request) -> no double-count, no leak
        if (runIdToShards.containsKey(runId) || runIdToRunSlot.containsKey(runId)) {
            log.debug("[CAPACITY] RunId {} already reserved, treating as success (idempotent)", runId);
            return true;
        }

        // Cap concurrent execute attempts (handles request floods in shared-pool mode)
        boolean executePermitAcquired = false;
        if (executeSemaphore != null && !executeSemaphore.tryAcquire()) {
            executeLimitRejectionReason.set("Too many execute requests in progress (max " + maxConcurrentExecuteRequests + "). Retry later.");
            log.debug("[CAPACITY] Max concurrent execute requests reached, runId={}", runId);
            return false;
        }
        executePermitAcquired = true;
        boolean reserved = false; // true when we actually reserved (so we keep the execute permit until release)

        try {
            if (exclusive) {
                int maxShards = getMaxShardsLimit();
                if (maxShards <= 0) {
                    reserved = true;
                    return true;
                }
                // Exclusive requires no other run active: shared runs use run slots (no shards reserved), so reject if any run slot in use
                int sharedInProgress = runSlotsInUse.get();
                if (sharedInProgress > 0) {
                    int maxRuns = guardrailProperties.getMaxConcurrentRuns();
                    String reason = maxRuns > 0
                            ? String.format("Exclusive load not available: %d of %d shared run(s) in progress. Retry later or use exclusive=false.", sharedInProgress, maxRuns)
                            : String.format("Exclusive load not available: %d shared run(s) in progress. Retry later or use exclusive=false.", sharedInProgress);
                    exclusiveRejectionReason.set(reason);
                    log.debug("[CAPACITY] Exclusive rejected: {} shared run(s) in progress; wait for them to finish or use exclusive=false, runId={}", sharedInProgress, runId);
                    return false;
                }
                int toReserve = maxShards;
                int current;
                int next;
                do {
                    current = shardsInUse.get();
                    next = current + toReserve;
                    if (next > maxShards) {
                        log.debug("[CAPACITY] Exclusive: pool full (in use {}, max {}), runId={}", current, maxShards, runId);
                        return false;
                    }
                } while (!shardsInUse.compareAndSet(current, next));
                // Put after CAS so release can always find; putIfAbsent avoids overwriting if same runId reserved twice (shouldn't after above check)
                if (runIdToShards.putIfAbsent(runId, toReserve) != null) {
                    // Corner case: another thread reserved same runId (shouldn't with unique runIds) -> rollback count
                    shardsInUse.addAndGet(-toReserve);
                    reserved = true;
                    return true;
                }
                reserved = true;
                log.debug("[CAPACITY] Reserved full pool ({}) for exclusive runId={}", toReserve, runId);
                return true;
            }

            // Shared: reserve one run slot (capped by max-concurrent-runs)
            int maxRuns = guardrailProperties.getMaxConcurrentRuns();
            if (maxRuns <= 0) {
                // Put first so only one thread wins for this runId; then increment (no cap)
                if (runIdToRunSlot.putIfAbsent(runId, Boolean.TRUE) != null) {
                    reserved = true;
                    return true;
                }
                runSlotsInUse.incrementAndGet();
                reserved = true;
                log.debug("[CAPACITY] Shared run slot for runId={} (no max-concurrent-runs cap)", runId);
                return true;
            }
            int current;
            int next;
            do {
                current = runSlotsInUse.get();
                next = current + 1;
                if (next > maxRuns) {
                    log.debug("[CAPACITY] Run slots full: {} in use, max {}, runId={}", current, maxRuns, runId);
                    return false;
                }
            } while (!runSlotsInUse.compareAndSet(current, next));
            if (runIdToRunSlot.putIfAbsent(runId, Boolean.TRUE) != null) {
                // Corner case: same runId won slot from another thread -> rollback slot count
                runSlotsInUse.decrementAndGet();
                reserved = true;
                return true;
            }
            reserved = true;
            log.debug("[CAPACITY] Reserved run slot for runId={}, slots in use {}", runId, next);
            return true;
        } finally {
            if (executePermitAcquired && !reserved && executeSemaphore != null) {
                executeSemaphore.release();
            }
        }
    }

    /**
     * Releases shards or run slot reserved for this run. Idempotent: safe to call multiple times for the same runId
     * (only the first call decrements counts; subsequent calls no-op). Prevents double-release race.
     * When max-concurrent-execute-requests is set, also releases one execute permit so another request can proceed.
     */
    public void release(String runId) {
        if (runId == null) return;
        Integer released = runIdToShards.remove(runId);
        boolean hadRunSlot = runIdToRunSlot.remove(runId) != null;
        if (released != null && released > 0) {
            shardsInUse.addAndGet(-released);
            log.debug("[CAPACITY] Released {} shards for runId={}", released, runId);
        }
        if (hadRunSlot) {
            runSlotsInUse.decrementAndGet();
            log.debug("[CAPACITY] Released run slot for runId={}", runId);
        }
        if ((released != null && released > 0) || hadRunSlot) {
            if (executeSemaphore != null) {
                executeSemaphore.release();
            }
        }
    }

    public int getShardsInUse() {
        return shardsInUse.get();
    }

    public int getAvailableShards() {
        int max = getMaxShardsLimit();
        if (max <= 0) return Integer.MAX_VALUE;
        return Math.max(0, max - shardsInUse.get());
    }

    public int getMaxShards() {
        return getMaxShardsLimit();
    }

    /** Current run slots in use (shared mode). */
    public int getRunSlotsInUse() {
        return runSlotsInUse.get();
    }

    /** Available run slots when max-concurrent-runs is set. */
    public int getAvailableRunSlots() {
        int max = guardrailProperties.getMaxConcurrentRuns();
        if (max <= 0) return Integer.MAX_VALUE;
        return Math.max(0, max - runSlotsInUse.get());
    }

    /**
     * Returns and clears the exclusive-rejection reason for the current thread (set when exclusive was rejected due to shared runs in progress).
     * Use in 429 response so the client sees "Retry later or use exclusive=false".
     */
    public static String getAndClearExclusiveRejectionReason() {
        String reason = exclusiveRejectionReason.get();
        if (reason != null) exclusiveRejectionReason.remove();
        return reason;
    }

    /**
     * Returns and clears the execute-limit rejection reason (set when max-concurrent-execute-requests was reached).
     */
    public static String getAndClearExecuteLimitRejectionReason() {
        String reason = executeLimitRejectionReason.get();
        if (reason != null) executeLimitRejectionReason.remove();
        return reason;
    }
}
