package com.di.streamnova.agent.workflow.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Holds all placeholder values for pipeline-db-to-gcs-template.yml.
 * Used to render the template at runtime from event configs, candidate (planner), and request.
 * All placeholders in the template must be covered here; missing ones use safe defaults.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DbToGcsTemplateContext {

    // ----- Run identity -----
    private String runId;
    /** Date for partitioning, e.g. yyyy-MM-dd */
    private String datePartition;

    // ----- Source (DB) - execute block and config.sources.db_source -----
    /** Display/correlation key, e.g. postgre_db, oracle_db */
    private String sourceKey;
    /** Handler type: postgres, oracle */
    private String sourceType;
    private String driverClass;
    private String jdbcUrl;
    private String username;
    private String password;
    private String sourceSchema;
    private String sourceTable;
    private String sourceQuery;

    // ----- Read tuning -----
    private int fetchSize;
    private int maxDbConnections;
    private int minimumIdle;
    private long connectionTimeoutMs;
    private long idleTimeoutMs;
    private long maxLifetimeMs;

    // ----- Parallelism (from planner/candidate) -----
    private int workers;
    private int shards;

    // ----- Incremental extraction -----
    private String extractionMode;
    private String incrementalColumn;
    private String watermarkFrom;
    private String watermarkTo;

    // ----- Profiling / validation -----
    /** defaultBytesPerRow as numeric bytes or string e.g. 204800 */
    private String defaultBytesPerRowBytes;
    private boolean enableSourceRowCount;
    private String sourceCountQuery;

    // ----- Destination (GCS) -----
    private String outputBucket;
    private String outputBasePath;
    private String fileFormat;
    private String compression;
    private int targetFileSizeMb;
    private String writeMode;
    private boolean traceEnabled;

    /**
     * Returns a value safe for template substitution (no null; empty string or default).
     */
    public String getRunIdOrEmpty() {
        return runId != null ? runId : "";
    }

    public String getDatePartitionOrEmpty() {
        return datePartition != null ? datePartition : "";
    }

    public String getSourceKeyOrEmpty() {
        return sourceKey != null ? sourceKey : "";
    }

    public String getSourceTypeOrEmpty() {
        return sourceType != null ? sourceType : "";
    }

    public String getDriverClassOrEmpty() {
        return driverClass != null ? driverClass : "";
    }

    public String getJdbcUrlOrEmpty() {
        return jdbcUrl != null ? jdbcUrl : "";
    }

    public String getUsernameOrEmpty() {
        return username != null ? username : "";
    }

    public String getPasswordOrEmpty() {
        return password != null ? password : "";
    }

    public String getSourceSchemaOrEmpty() {
        return sourceSchema != null ? sourceSchema : "";
    }

    public String getSourceTableOrEmpty() {
        return sourceTable != null ? sourceTable : "";
    }

    public String getSourceQueryOrEmpty() {
        return sourceQuery != null ? sourceQuery : "";
    }

    public String getExtractionModeOrEmpty() {
        return extractionMode != null ? extractionMode : "FULL";
    }

    public String getIncrementalColumnOrEmpty() {
        return incrementalColumn != null ? incrementalColumn : "";
    }

    public String getWatermarkFromOrEmpty() {
        return watermarkFrom != null ? watermarkFrom : "";
    }

    public String getWatermarkToOrEmpty() {
        return watermarkTo != null ? watermarkTo : "";
    }

    public String getDefaultBytesPerRowBytesOrDefault() {
        return defaultBytesPerRowBytes != null && !defaultBytesPerRowBytes.isBlank() ? defaultBytesPerRowBytes : "204800";
    }

    public String getSourceCountQueryOrEmpty() {
        return sourceCountQuery != null ? sourceCountQuery : "";
    }

    public String getOutputBucketOrEmpty() {
        return outputBucket != null ? outputBucket : "";
    }

    public String getOutputBasePathOrEmpty() {
        return outputBasePath != null ? outputBasePath : "";
    }

    public String getFileFormatOrEmpty() {
        return fileFormat != null ? fileFormat : "PARQUET";
    }

    public String getCompressionOrEmpty() {
        return compression != null ? compression : "SNAPPY";
    }

    public String getWriteModeOrEmpty() {
        return writeMode != null ? writeMode : "APPEND";
    }
}
