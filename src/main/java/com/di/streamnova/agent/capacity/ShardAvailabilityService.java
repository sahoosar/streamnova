package com.di.streamnova.agent.capacity;

import com.di.streamnova.config.PipelineConfigService;
import com.di.streamnova.config.PipelineConfigSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tracks shard usage: reserve shards when a load starts, release when it finishes.
 * Max shards: when streamnova.capacity.max-shards &gt; 0 use it; otherwise derived from pipeline_config.yml maximumPoolSize (single source of truth).
 */
@Slf4j
@Service
public class ShardAvailabilityService {

    @Value("${streamnova.capacity.max-shards}")
    private int maxShardsOverride;

    private final PipelineConfigService pipelineConfigService;
    private final AtomicInteger shardsInUse = new AtomicInteger(0);
    private final ConcurrentHashMap<String, Integer> runIdToShards = new ConcurrentHashMap<>();

    public ShardAvailabilityService(PipelineConfigService pipelineConfigService) {
        this.pipelineConfigService = pipelineConfigService;
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
     * Tries to reserve shards for a run. When max is 0 (no limit), always allows.
     *
     * @param requiredShards shards needed for this execution
     * @param runId          unique id for this run (used to release later)
     * @return true if reserved (or no limit), false if not enough shards available
     */
    public boolean tryReserve(int requiredShards, String runId) {
        if (requiredShards <= 0) return true;
        if (runId == null || runId.isBlank()) return false;
        int maxShards = getMaxShardsLimit();
        if (maxShards <= 0) return true; // no limit

        int current;
        int next;
        do {
            current = shardsInUse.get();
            next = current + requiredShards;
            if (next > maxShards) {
                log.debug("[CAPACITY] Shards not available: need {}, in use {}, max {}", requiredShards, current, maxShards);
                return false;
            }
        } while (!shardsInUse.compareAndSet(current, next));

        runIdToShards.put(runId, requiredShards);
        log.debug("[CAPACITY] Reserved {} shards for runId={}, in use {}", requiredShards, runId, next);
        return true;
    }

    /**
     * Releases shards reserved for this run. Safe to call multiple times for the same runId.
     */
    public void release(String runId) {
        if (runId == null) return;
        Integer released = runIdToShards.remove(runId);
        if (released != null && released > 0) {
            shardsInUse.addAndGet(-released);
            log.debug("[CAPACITY] Released {} shards for runId={}", released, runId);
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
}
