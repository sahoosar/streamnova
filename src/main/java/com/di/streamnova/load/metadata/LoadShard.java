package com.di.streamnova.load.metadata;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * Domain model for the {@code load_shards} table.
 *
 * <p>One row per shard range.  Created in bulk when the final plan is
 * committed; updated as each shard is read and written to GCS.
 *
 * <pre>Status flow: PENDING → RUNNING → DONE | FAILED | SKIPPED</pre>
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class LoadShard {

    private String  id;
    private String  runId;
    private int     shardIndex;

    /** Planning iteration that produced this shard range (1, 2, or 3). */
    private int     iterationBorn;

    // ---- range -------------------------------------------------------------
    private String  partitionCol;
    private Long    minKey;
    private Long    maxKey;
    private Long    estimatedRows;

    // ---- output ------------------------------------------------------------
    private Long    actualRowCount;
    private String  gcsPath;
    private Long    gcsFileSizeBytes;

    // ---- lifecycle ---------------------------------------------------------
    private String  status;
    private int     attempt;
    private String  errorMessage;

    private Instant startedAt;
    private Instant completedAt;
    private Long    durationMs;
}
