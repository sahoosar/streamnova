package com.di.streamnova.agent.capacity;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tracks shard usage: reserve shards when a load starts, release when it finishes.
 * When max-shards is set, execute is allowed only if requiredShards <= (maxShards - shardsInUse).
 */
@Slf4j
@Service
public class ShardAvailabilityService {

    @Value("${streamnova.capacity.max-shards:0}")
    private int maxShards;

    private final AtomicInteger shardsInUse = new AtomicInteger(0);
    private final ConcurrentHashMap<String, Integer> runIdToShards = new ConcurrentHashMap<>();

    /**
     * Tries to reserve shards for a run. When max-shards is 0, always allows (no limit).
     *
     * @param requiredShards shards needed for this execution
     * @param runId          unique id for this run (used to release later)
     * @return true if reserved (or no limit), false if not enough shards available
     */
    public boolean tryReserve(int requiredShards, String runId) {
        if (requiredShards <= 0) return true;
        if (runId == null || runId.isBlank()) return false;
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
        if (maxShards <= 0) return Integer.MAX_VALUE;
        return Math.max(0, maxShards - shardsInUse.get());
    }

    public int getMaxShards() {
        return maxShards;
    }
}
