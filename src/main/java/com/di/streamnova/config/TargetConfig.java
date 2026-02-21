package com.di.streamnova.config;

import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Pipeline destination/sink. User choice: type can be bigquery, gcs, etc. — no fixed handler for target.
 * YAML: pipeline.config.target
 * <p>
 * {@link #type} is the destination handler type (e.g. bigquery, gcs). At execution start the pipeline
 * validates by type (e.g. bigquery → dataset+table; gcs → gcsPath) and executes accordingly.
 */
@Data
@NoArgsConstructor
public class TargetConfig {
    /** Target/destination handler type (e.g. bigquery, gcs). User selects; pipeline validates and runs by type. */
    private String type;
    /** BigQuery project (when type=bigquery). Used when writing into BQ (db-to-gcs-to-bq, gcs-to-bq). */
    private String project;
    /** BigQuery dataset (required when type=bigquery). */
    private String dataset;
    /** BigQuery table (required when type=bigquery). */
    private String table;
    /** BigQuery create disposition (e.g. CREATE_IF_NEEDED, CREATE_NEVER). Used when writing into BQ. */
    private String createDisposition;
    /** GCS path for destination (required when type=gcs). e.g. gs://bucket/export/ */
    private String gcsPath;
    /** GCS bucket name (optional; can be derived from gcsPath). Used by db-to-gcs template. */
    private String bucket;
    /** GCS base path inside bucket (optional). Used by db-to-gcs template. */
    private String basePath;
    /** Output file format for db-to-gcs template (e.g. PARQUET, AVRO, CSV, JSONL). */
    private String fileFormat;
    /** Compression for db-to-gcs template (e.g. SNAPPY, GZIP). */
    private String compression;
    /** Target file size in MB for db-to-gcs template. */
    private Integer targetFileSizeMb;
    /** Write mode for db-to-gcs template (APPEND, OVERWRITE). */
    private String writeMode;
    /** Whether to add trace/lineage columns for db-to-gcs template. */
    private Boolean traceEnabled;
    /** Path to schema file (e.g. GCS). */
    private String schemaPath;
    /** Load mode (e.g. load_from_gcs). */
    private String loadMode;
    /** Source format (e.g. PARQUET). */
    private String sourceFormat;
    /** Write disposition (e.g. WRITE_APPEND). */
    private String writeDisposition;
}
