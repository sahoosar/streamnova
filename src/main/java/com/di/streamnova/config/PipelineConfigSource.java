package com.di.streamnova.config;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.ArrayList;
import java.util.List;

/**
 * Effective source config for one DB read: connection (JDBC + pool) plus table/shard/partition options.
 * Comes from pipeline.config.connections.&lt;name&gt; (connection only) or from merging a connection with
 * pipeline.config.tables.&lt;key&gt; when the API uses ?source=tableKey.
 */
@Data
@NoArgsConstructor
public class PipelineConfigSource {

    // ----- Connection (required for pool and query; for type=gcs only gcsPath is used) -----
    private String type;
    private String driver;
    private String jdbcUrl;
    private String username;
    @ToString.Exclude
    private String password;
    private String table;
    /** Optional schema (e.g. public); used with table for qualified identifier. */
    private String schema;
    /** BigQuery project (when type=bigquery). Used by bq-to-gcs template. */
    private String project;
    /** BigQuery dataset (when type=bigquery). Used by bq-to-gcs template. */
    private String dataset;
    /** BigQuery location (when type=bigquery). Used by bq-to-gcs template. */
    private String location;
    /** Optional query (e.g. BigQuery SQL); overrides table when set. Used by bq-to-gcs template. */
    private String query;
    /** GCS path for source when type=gcs (e.g. gs://bucket/prefix*.parquet). Use gcsPaths or gcsLocations for multiple. */
    private String gcsPath;
    /** Multiple GCS paths only (optional). Use gcsLocations when each location has a different format (parquet, json, csv). */
    private List<String> gcsPaths;
    /** Multiple GCS locations with path and format per folder (parquet, json, csv, etc.). When set, use instead of gcsPath/gcsPaths. */
    private List<GcsSourceLocation> gcsLocations;

    // ----- Query execution -----
    private int fetchSize;
    private Double fetchFactor;
    private String defaultBytesPerRow;

    // ----- Table / sharding (optional) -----
    private String upperBoundColumn;   // Column for hash-based sharding or partition filter
    private String partitionValue;     // Partition value to filter (single value: date, id, string; use with upperBoundColumn)
    private String partitionStartValue; // Start of range (e.g. date); use with partitionEndValue and upperBoundColumn for date-range load
    private String partitionEndValue;  // End of range (e.g. date); use with partitionStartValue and upperBoundColumn for date-range load
    private String shardColumn;        // Fallback shard column when no PK/partition
    private int maxColumns;
    private List<String> orderBy;

    // ----- Connection pool -----
    private int maximumPoolSize;
    private int fallbackPoolSize;
    private int minimumIdle;
    private long idleTimeout;
    private long connectionTimeout;
    private long maxLifetime;

    // ----- Timeouts (optional; null = no timeout) -----
    private Integer queryTimeout;
    private Integer socketTimeout;
    private Integer statementTimeout;

    // ----- Behaviour and overrides -----
    private Boolean enableProgressLogging;
    private Integer shards;
    /** When set and less than shards, limits concurrent DB connections (shift-based / phased load). */
    private Integer maxConcurrentShards;
    private Integer workers;
    private String machineType;

    /**
     * Returns the list of config attribute names that are missing or invalid for connection pool creation.
     * Table is not required for the pool; it is required only when reading data (pipeline.config.tables or connection.table).
     */
    public List<String> getMissingAttributesForConnection() {
        List<String> missing = new ArrayList<>();
        if (getType() == null || getType().isBlank()) missing.add("type");
        boolean isGcs = "gcs".equals(getType() != null ? getType().trim().toLowerCase() : "");
        if (isGcs) {
            if (getGcsPath() == null || getGcsPath().isBlank()) missing.add("gcsPath (required when type=gcs)");
            return missing;
        }
        if (getDriver() == null || getDriver().isBlank()) missing.add("driver");
        if (getJdbcUrl() == null || getJdbcUrl().isBlank()) missing.add("jdbcUrl");
        else if (!getJdbcUrl().trim().toLowerCase().startsWith("jdbc:")) missing.add("jdbcUrl (must start with jdbc:)");
        if (getUsername() == null || getUsername().isBlank()) missing.add("username");
        if (getPassword() == null || getPassword().isBlank()) missing.add("password");
        int maxPoolSize = getMaximumPoolSize() > 0 ? getMaximumPoolSize() : getFallbackPoolSize();
        if (maxPoolSize <= 0) missing.add("maximumPoolSize or fallbackPoolSize");
        return missing;
    }

    /**
     * Builds a DbConfigSnapshot for connection pool creation (e.g. HikariDataSource.getOrInit).
     * Table is not required here; it is required only when reading data (set in pipeline.config.tables or on connection).
     */
    public DbConfigSnapshot toDbConfigSnapshot() {
        if (getType() == null || getType().isBlank()) {
            throw new IllegalArgumentException("pipeline.config.source.type is required in pipeline_config.yml (e.g. postgres, oracle)");
        }
        if (getDriver() == null || getDriver().isBlank()) {
            throw new IllegalArgumentException("pipeline.config.source.driver is required in pipeline_config.yml (e.g. org.postgresql.Driver)");
        }
        if (getJdbcUrl() == null || getJdbcUrl().isBlank()) {
            throw new IllegalArgumentException("pipeline.config.source.jdbcUrl is required in pipeline_config.yml (e.g. jdbc:postgresql://localhost:5432/marketdb)");
        }
        if (!getJdbcUrl().trim().toLowerCase().startsWith("jdbc:")) {
            throw new IllegalArgumentException("pipeline.config.source.jdbcUrl must start with jdbc: (e.g. jdbc:postgresql://host:port/db)");
        }
        if (getUsername() == null || getUsername().isBlank()) {
            throw new IllegalArgumentException("pipeline.config.source.username is required in pipeline_config.yml");
        }
        if (getPassword() == null || getPassword().isBlank()) {
            throw new IllegalArgumentException("pipeline.config.source.password is required and must be non-empty in pipeline_config.yml (or set via env var, e.g. ${POSTGRES_PASSWORD})");
        }
        int maxPoolSize = getMaximumPoolSize() > 0 ? getMaximumPoolSize() : getFallbackPoolSize();
        if (maxPoolSize <= 0) {
            throw new IllegalArgumentException("pipeline.config.source.maximumPoolSize (or fallbackPoolSize) is required and must be > 0 in pipeline_config.yml");
        }
        if (getIdleTimeout() <= 0) {
            throw new IllegalArgumentException("pipeline.config.source.idleTimeout is required and must be > 0 in pipeline_config.yml");
        }
        if (getConnectionTimeout() <= 0) {
            throw new IllegalArgumentException("pipeline.config.source.connectionTimeout is required and must be > 0 in pipeline_config.yml");
        }
        if (getMaxLifetime() <= 0) {
            throw new IllegalArgumentException("pipeline.config.source.maxLifetime is required and must be > 0 in pipeline_config.yml");
        }
        int minIdle = getMinimumIdle() > 0 ? Math.min(getMinimumIdle(), maxPoolSize) : Math.min(2, maxPoolSize);
        return new DbConfigSnapshot(
                getJdbcUrl(),
                getUsername(),
                getPassword(),
                getDriver(),
                maxPoolSize,
                minIdle,
                getIdleTimeout(),
                getConnectionTimeout(),
                getMaxLifetime()
        );
    }
}
