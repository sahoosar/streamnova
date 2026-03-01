package com.di.streamnova.load.manifest;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.List;

/**
 * JSON-serialisable manifest that is written to GCS after Stage 1 and Stage 3.
 *
 * <p>Each manifest entry describes one JSONL shard file so that downstream
 * consumers (Stage 2 validation, BigQuery load) can enumerate the files
 * deterministically without listing the GCS bucket.
 *
 * <p>The manifest itself is persisted to GCS as
 * {@code gs://{bucket}/{prefix}/{stage}/manifest.json}.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class GcsManifest {

    private String       runId;
    private String       stage;               // STAGE1 or STAGE3
    private String       tableSchema;
    private String       tableName;
    private Instant      createdAt;

    private int          totalFiles;
    private long         totalRows;
    private long         totalBytes;

    private List<Entry>  files;

    /* ---------------------------------------------------------------------- */

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Entry {

        /** Zero-based shard index (matches {@code load_shards.shard_index}). */
        private int    shardIndex;

        /** Full GCS path including {@code gs://} scheme. */
        private String gcsPath;

        /** Actual rows written to this file. */
        private long   rowCount;

        /** GCS object size in bytes. */
        private long   sizeBytes;
    }
}
