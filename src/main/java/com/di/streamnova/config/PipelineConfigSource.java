package com.di.streamnova.config;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.ArrayList;
import java.util.List;

@Data
@NoArgsConstructor
public class PipelineConfigSource {
    private String type;
    private String driver;
    private String jdbcUrl;
    private String username;
    @ToString.Exclude
    private String password;
    private String table;
    private int fetchSize;
    private String upperBoundColumn;
    private String partitionValue;   // optional - partition value to filter data (supports any data type: date, integer, string, etc.)
                                     // Use when: Table is partitioned and you want to read only a specific partition
                                     // Format: Value format depends on column data type:
                                     //   - Date: "2024-01-15", "2024/01/15", "15-01-2024", etc. (supports multiple formats)
                                     //   - Integer/BigInt: "12345", "1000000"
                                     //   - String/Varchar: "partition_2024_01", "region_east"
                                     //   - Timestamp: "2024-01-15 10:30:00"
                                     // Works with: upperBoundColumn (partition column name) to filter by specific partition value
                                     // Example: upperBoundColumn: "created_date", partitionValue: "2024-01-15"
                                     // Example: upperBoundColumn: "region_id", partitionValue: "5"
                                     // Example: upperBoundColumn: "region_name", partitionValue: "east"
                                     // ✅ Auto-detects column type and formats value appropriately
    private String shardColumn;      // optional - user-provided shard/ordering column for fallback sharding
                                     // If provided, will be used when no stable keys (PK/index/partition) are found
                                     // Can be overridden via command line: --pipeline.config.source.shardColumn=column_name
    private int maxColumns;
    private List<String> orderBy;
    private int maximumPoolSize ;  // default 8; when machineType missing use this, else use fallbackPoolSize if 0
    private int fallbackPoolSize ; // when machineType missing AND maximumPoolSize=0; also when vCPUs cannot be parsed
    private int minimumIdle;
    private long idleTimeout ;      //  ms
    private long connectionTimeout; // ms
    private long maxLifetime ;     // ms
    private Integer queryTimeout;   // optional - query timeout in seconds (for large partition column datasets)
                                    //           Default: null (no timeout). Recommended: 3600 (1 hour) for huge datasets
    private Integer socketTimeout;  // optional - socket timeout in seconds (for network operations)
                                    //           Default: null (no timeout). Recommended: 300 (5 minutes) for large datasets
    private Integer statementTimeout; // optional - statement timeout in seconds (PostgreSQL specific)
                                     //            Default: null (no timeout). Recommended: 3600 (1 hour) for huge datasets
    private Boolean enableProgressLogging; // optional - enable progress logging for large datasets (default: true)
                                           //            Set to false to reduce log volume for very large tables
    private Integer shards;        // optional - user-provided shard count (overrides calculation)
    private Integer workers;       // optional - user-provided worker count (overrides calculation)
    private String machineType;    // optional - user-provided machine type (e.g., "n2-standard-4", "n2-highcpu-8")
    private Double  fetchFactor;   // optional (e.g., 1.25)
    private String defaultBytesPerRow; // optional (e.g., "1KB") for row size estimation

    /**
     * Returns the list of config attribute names that are missing or invalid for datasource connection.
     * Empty list means all required attributes are present and valid.
     */
    public List<String> getMissingAttributesForConnection() {
        List<String> missing = new ArrayList<>();
        if (getType() == null || getType().isBlank()) missing.add("type");
        if (getDriver() == null || getDriver().isBlank()) missing.add("driver");
        if (getJdbcUrl() == null || getJdbcUrl().isBlank()) missing.add("jdbcUrl");
        else if (!getJdbcUrl().trim().toLowerCase().startsWith("jdbc:")) missing.add("jdbcUrl (must start with jdbc:)");
        if (getUsername() == null || getUsername().isBlank()) missing.add("username");
        if (getPassword() == null || getPassword().isBlank()) missing.add("password");
        if (getTable() == null || getTable().isBlank()) missing.add("table");
        int maxPoolSize = getMaximumPoolSize() > 0 ? getMaximumPoolSize() : getFallbackPoolSize();
        if (maxPoolSize <= 0) missing.add("maximumPoolSize or fallbackPoolSize");
        if (getIdleTimeout() <= 0) missing.add("idleTimeout");
        if (getConnectionTimeout() <= 0) missing.add("connectionTimeout");
        if (getMaxLifetime() <= 0) missing.add("maxLifetime");
        return missing;
    }

    /**
     * Builds a DbConfigSnapshot from pipeline config (e.g. for HikariDataSource.getOrInit).
     * Validates required fields and uses pool and timeout values from config as-is.
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
        if (getTable() == null || getTable().isBlank()) {
            throw new IllegalArgumentException("pipeline.config.source.table is required in pipeline_config.yml (e.g. market_summary)");
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
