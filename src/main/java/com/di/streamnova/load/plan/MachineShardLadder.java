package com.di.streamnova.load.plan;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Maps a GCE / Dataflow machine type to the <em>target bytes per shard</em>.
 *
 * <h3>Calibration basis — size-based, not row-based</h3>
 * <pre>
 *  target_bytes_per_shard
 *      = observed_throughput_MB_per_s  ×  1_048_576  ×  target_shard_duration_s
 *
 *  Throughput is measured per <em>connection</em> (not per worker): Stage 1
 *  is pool-limited, so one JDBC connection handles exactly one concurrent shard.
 *
 *  Baseline (180 s target, PostgreSQL JDBC streaming cursor):
 *  ┌──────────────────────┬──────────┬─────────────────┬──────────────────┐
 *  │ Machine type         │ vCPUs    │ MB/s per conn   │ Target MB/shard  │
 *  ├──────────────────────┼──────────┼─────────────────┼──────────────────┤
 *  │ n2d-standard-2       │   2      │   ~0.8          │    128           │
 *  │ n2d-standard-4       │   4      │   ~1.5          │    256           │
 *  │ n2d-standard-8       │   8      │   ~3.0          │    512           │
 *  │ n2d-standard-16      │  16      │   ~6.0          │   1024           │
 *  │ n2d-standard-32      │  32      │   ~9.0          │   1536           │
 *  │ n2d-standard-48      │  48      │  ~12.0          │   2048           │
 *  │ n2d-standard-64      │  64      │  ~14.0          │   2304           │
 *  │ n2d-standard-96      │  96      │  ~18.0          │   3072           │
 *  │ n2-standard-4        │   4      │   ~1.2          │    256           │
 *  │ n2-standard-8        │   8      │   ~2.5          │    512           │
 *  │ n2-standard-16       │  16      │   ~5.0          │    896           │
 *  │ n2-standard-32       │  32      │   ~8.0          │   1280           │
 *  │ c3-standard-4        │   4      │   ~1.4          │    256           │
 *  │ c3-standard-8        │   8      │   ~3.0          │    512           │
 *  │ c3-standard-22       │  22      │   ~7.0          │   1280           │
 *  │ c3-standard-44       │  44      │  ~12.0          │   2048           │
 *  └──────────────────────┴──────────┴─────────────────┴──────────────────┘
 *
 *  The warm-up probe (iteration 2) and the pilot shard (iteration 3) override
 *  these defaults with <em>measured</em> values from the actual table.
 * </pre>
 *
 * <h3>Example — 10 M rows × 500 B avg = ~5 GB on n2d-standard-8</h3>
 * <pre>
 *   targetBytesPerShard = 512 MB
 *   shardCount          = ⌈5 120 MB / 512 MB⌉ = 10
 *   maxConcurrent       = min(10, poolSize=10) = 10   ← fills the pool exactly in one wave
 * </pre>
 */
@Component
@Slf4j
public class MachineShardLadder {

    private static final long MB = 1024L * 1024L;

    /** machine-type (lower-case) → target bytes per shard */
    private static final Map<String, Long> LADDER = Map.ofEntries(

        // ── n2d family ─────────────────────────────────────────────────
        Map.entry("n2d-standard-2",    128L * MB),
        Map.entry("n2d-standard-4",    256L * MB),
        Map.entry("n2d-standard-8",    512L * MB),
        Map.entry("n2d-standard-16",  1024L * MB),
        Map.entry("n2d-standard-32",  1536L * MB),
        Map.entry("n2d-standard-48",  2048L * MB),
        Map.entry("n2d-standard-64",  2304L * MB),
        Map.entry("n2d-standard-96",  3072L * MB),

        // ── n2 family ───────────────────────────────────────────────────
        Map.entry("n2-standard-2",     128L * MB),
        Map.entry("n2-standard-4",     256L * MB),
        Map.entry("n2-standard-8",     512L * MB),
        Map.entry("n2-standard-16",    896L * MB),
        Map.entry("n2-standard-32",   1280L * MB),
        Map.entry("n2-standard-48",   1792L * MB),
        Map.entry("n2-standard-64",   2048L * MB),
        Map.entry("n2-standard-96",   2816L * MB),

        // ── c3 family ───────────────────────────────────────────────────
        Map.entry("c3-standard-4",     256L * MB),
        Map.entry("c3-standard-8",     512L * MB),
        Map.entry("c3-standard-22",   1280L * MB),
        Map.entry("c3-standard-44",   2048L * MB),
        Map.entry("c3-standard-88",   3584L * MB),
        Map.entry("c3-standard-176",  5120L * MB)
    );

    private static final long   DEFAULT_BYTES_PER_SHARD = 512L * MB;  // 512 MB
    private static final String DEFAULT_MACHINE_TYPE    = "n2d-standard-8";

    /* ------------------------------------------------------------------ */

    /**
     * Returns the target <strong>bytes per shard</strong> for the given machine type.
     *
     * <p>Falls back to 512 MB for unknown or null machine types.
     */
    public long getTargetBytesPerShard(String machineType) {
        if (machineType == null || machineType.isBlank()) {
            log.debug("[LADDER] no machine type → default {} MB/shard",
                      DEFAULT_BYTES_PER_SHARD / MB);
            return DEFAULT_BYTES_PER_SHARD;
        }
        String key   = machineType.toLowerCase().trim();
        Long   exact = LADDER.get(key);
        if (exact != null) {
            log.debug("[LADDER] {} → {} MB/shard", key, exact / MB);
            return exact;
        }
        // Prefix match handles variants like "n2d-standard-8-lssd"
        for (Map.Entry<String, Long> e : LADDER.entrySet()) {
            if (key.startsWith(e.getKey())) {
                log.debug("[LADDER] prefix match '{}' → {} MB/shard",
                          e.getKey(), e.getValue() / MB);
                return e.getValue();
            }
        }
        log.warn("[LADDER] unknown machine type '{}' → fallback {} MB/shard",
                 machineType, DEFAULT_BYTES_PER_SHARD / MB);
        return DEFAULT_BYTES_PER_SHARD;
    }

    /** Convenience: target in MB (double) — useful for log messages. */
    public double getTargetMbPerShard(String machineType) {
        return getTargetBytesPerShard(machineType) / (double) MB;
    }

    public String getDefaultMachineType() {
        return DEFAULT_MACHINE_TYPE;
    }
}
