package com.di.streamnova.config;

import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Staging/intermediate storage. User choice: handlerType can be gcs, bigquery, etc. — no fixed handler for intermediate.
 * YAML: pipeline.config.intermediate
 * <p>
 * When {@link #handlerType} is set (e.g. gcs, bigquery), the pipeline validates by type and executes accordingly.
 * If unset, derived from gcsPath (gcs when gcsPath is set).
 */
@Data
@NoArgsConstructor
public class IntermediateConfig {
    /**
     * Handler type for the middle/staging layer (e.g. gcs, bigquery). User selects; pipeline validates by type.
     */
    private String handlerType;
    /** Data format for staged files (e.g. parquet). */
    private String dataFormat;
    /** GCS path for staging (required when handlerType=gcs). e.g. gs://bucket/stage/ */
    private String gcsPath;
    /** GCS bucket for staging (optional; can be derived from gcsPath). Feeds stageBucket in db-to-gcs-to-bq template. */
    private String bucket;
    /** GCS region for staging (optional). Feeds stageRegion in db-to-gcs-to-bq template. */
    private String region;
    /** GCS base path inside bucket (optional). Feeds stageBasePath in db-to-gcs-to-bq template. */
    private String basePath;
    /** Output file format for staged files (e.g. PARQUET, AVRO). Feeds fileFormat in db-to-gcs-to-bq template. */
    private String fileFormat;
    /** Compression for staged files (e.g. SNAPPY, GZIP). */
    private String compression;
    /** Target file size in MB for staged files. */
    private Integer targetFileSizeMb;
    /** Write mode for staging (APPEND, OVERWRITE). */
    private String writeMode;
    /** Whether to add trace/lineage columns for staged files. */
    private Boolean traceEnabled;
    /** BigQuery dataset for intermediate (required when handlerType=bigquery). */
    private String dataset;
    /** BigQuery table for intermediate (required when handlerType=bigquery). */
    private String table;

    /**
     * Resolves the effective intermediate handler type: explicit handlerType, or gcs when gcsPath is set.
     */
    public String getEffectiveHandlerType() {
        if (handlerType != null && !handlerType.isBlank()) {
            return handlerType.trim().toLowerCase();
        }
        if (gcsPath != null && !gcsPath.isBlank()) {
            return "gcs";
        }
        return null;
    }
}
