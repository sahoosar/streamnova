package com.di.streamnova.tuning;

import lombok.extern.slf4j.Slf4j;

/**
 * A plan for sharding a data processing job, including a method to compute
 * per-shard fetch sizes based on resource constraints and performance targets.
 */

@Slf4j
public class ShardPlan {
    public final String environment;
    public final int vCpus;
    public final long jvmHeapBytes;
    public final int shards;
    public final int partitions;
    public final int connectionPoolSize;
    public final int avgRowBytes;
    public final long bandwidthBytesPerSec;
    public final double rowBufferFraction;
    public final double targetBatchSeconds;
    public final int minFetch;
    public final int maxFetch;
    private final Double fetchFactor;

    ShardPlan(String env, int vCpus, long heap, int shards, int parts, int pool,
              int avgRowBytes, long bps, double rowBufFrac, double tgtBatchSec,
              int minFetch, int maxFetch, Double fetchFactor) {
        this.environment = env;
        this.vCpus = vCpus;
        this.jvmHeapBytes = heap;
        this.shards = shards;
        this.partitions = parts;
        this.connectionPoolSize = pool;
        this.avgRowBytes = avgRowBytes;
        this.bandwidthBytesPerSec = bps;
        this.rowBufferFraction = rowBufFrac;
        this.targetBatchSeconds = tgtBatchSec;
        this.minFetch = minFetch;
        this.maxFetch = maxFetch;
        this.fetchFactor = fetchFactor;
    }

    /** Per-shard fetch size (deterministic ±5% jitter to avoid lockstep). */
    public int fetchSizeForShard(int shardId) {
        int concurrency = Math.max(1, Math.min(Math.min(vCpus, shards), connectionPoolSize));
        long bufBudgetBytesTotal = (long) (jvmHeapBytes * rowBufferFraction);
        long bufPerConnBytes = Math.max(8L << 20, bufBudgetBytesTotal / concurrency);
        int rowSz = Math.max(64, avgRowBytes);

        long fs = computeFetchSize(shardId, bufPerConnBytes, rowSz);
        fs = Math.max(minFetch, Math.min(maxFetch, fs));
        return (int) fs;
    }

    private long computeFetchSize(int shardId, long bufPerConnBytes, int rowSz) {
        long rowsByMem = Math.max(1, bufPerConnBytes / rowSz);
        long targetBatchBytes = (long) (bandwidthBytesPerSec * targetBatchSeconds);
        long rowsByBw = Math.max(1, targetBatchBytes / rowSz);
        long base = Math.min(rowsByMem, rowsByBw);

        double skew = 1.0;
        if (partitions > shards * 2) skew = 0.8;
        else if (shards > partitions * 2) skew = 1.2;

        // tiny jitter (~±5%) so shards don't sync on boundaries
        double jitter = 0.95 + ((shardId * 0x9E3779B9) & 0xF) * (0.10 / 15.0);

        long fs = Math.round(base * skew * jitter);

        // apply quick-tune factor if present
        if (fetchFactor != null) {
            fs = Math.round(fs * fetchFactor);
        }

        if (log.isDebugEnabled()) {
            log.debug(
                    "shard {}: memBuf={}B, targetBatch={}B, baseRows={}, skew={}, jitter={}, factor={}, fetch={}",
                    shardId,
                    bufPerConnBytes,
                    targetBatchBytes,
                    base,
                    String.format("%.3f", skew),
                    String.format("%.3f", jitter),
                    (fetchFactor != null ? String.format("%.3f", fetchFactor) : "n/a"),
                    fs
            );
        }
        return fs;
    }
    public void logSummary() {
        log.info("ShardPlan: env={}, vCPUs={}, heap={}MiB, shards={}, partitions={}, pool={}, " +
                        "rowSz={}B, fetchFactor={}, fetch[min..max]={}..{}",
                environment,
                vCpus,
                jvmHeapBytes / (1024 * 1024),
                shards,
                partitions,
                connectionPoolSize,
                avgRowBytes,
                (fetchFactor != null ? String.format("%.2f", fetchFactor) : "1.0"),
                minFetch,
                maxFetch
        );
    }
}


