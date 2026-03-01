package com.di.streamnova.load.metadata;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * Domain model for the {@code load_manifests} table.
 *
 * <p>Written once per stage (Stage 1 and Stage 3) after all work for that
 * stage is complete.  Provides a durable record of what landed in GCS /
 * BigQuery for integrity audits.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class LoadManifest {

    private String  id;
    private String  runId;

    /** {@code STAGE1} or {@code STAGE3}. */
    private String  stage;

    /** GCS path of the JSON manifest file (e.g. {@code gs://bucket/prefix/stage1/manifest.json}). */
    private String  manifestGcsPath;

    /** Number of JSONL shard files included in this manifest. */
    private Integer totalFiles;

    /** Sum of actual row counts across all files. */
    private Long    totalRows;

    /** Sum of GCS file sizes in bytes. */
    private Long    totalBytes;

    private Instant createdAt;
}
