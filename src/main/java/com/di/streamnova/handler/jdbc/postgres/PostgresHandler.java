package com.di.streamnova.handler.jdbc.postgres;

import com.di.streamnova.config.DbConfigSnapshot;
import com.di.streamnova.config.PipelineConfigSource;
import com.di.streamnova.handler.SourceHandler;
import com.di.streamnova.handler.jdbc.postgres.PostgresDataQualityChecker;
import com.di.streamnova.handler.jdbc.postgres.PostgresPartitionValueFormatter;
import com.di.streamnova.handler.jdbc.postgres.PostgresSchemaDetector;
import com.di.streamnova.handler.jdbc.postgres.PostgresStatisticsEstimator;
import com.di.streamnova.util.ConnectionPoolLogger;
import com.di.streamnova.util.HikariDataSource;
import com.di.streamnova.util.InputValidator;
import com.di.streamnova.util.MetricsCollector;
import com.di.streamnova.util.TypeConverter;
import com.di.streamnova.agent.shardplanner.ShardPlanner;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.stream.IntStream;

/**
 * Handler for reading data from PostgreSQL with parallel sharded reads.
 *
 * <p><b>Sharding</b> – Rows are split across shards by a shard column (auto-detected or from config).
 * Priority: upperBoundColumn (config) → index → primary key → composite PK (first col) → partition key → shardColumn (config) → ordering column → ctid.
 * See {@link #buildShardedQuery} and {@link #detectStableShardColumn} for details and examples.
 *
 * <p><b>Partition filtering</b> – Optional WHERE to load only one partition or a range:
 * <ul>
 *   <li>Single value: partitionValue + upperBoundColumn (or auto-detected partition column) → e.g. {@code WHERE created_date = DATE '2024-01-15'}</li>
 *   <li>Range: partitionStartValue + partitionEndValue → e.g. {@code WHERE created_date >= DATE '2024-01-01' AND created_date <= DATE '2024-01-31'}</li>
 * </ul>
 *
 * <p>Other: TypeConverter for types, ShardPlanner for shard/worker count, shard_id in schema, row count validation.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class PostgresHandler implements SourceHandler<PipelineConfigSource> {

    private final MetricsCollector metricsCollector;
    private final ShardPlanner shardPlanner;
    private final AdaptiveFetchProperties adaptiveFetchProperties;

    @Override
    public String type() {
        return "postgres";
    }

    @Override
    public PCollection<Row> read(Pipeline pipeline, PipelineConfigSource config) {
        long readStartTime = System.currentTimeMillis();
        
        try {
            // 1. Validate inputs
            validateConfig(config);
            
            PostgresStatisticsEstimator.TableStatistics stats;
            DbConfigSnapshot dbConfig;

            // Always create DataSource with maximumPoolSize from pipeline_config.yml
            dbConfig = createDbConfigSnapshot(config);
            DataSource ds = HikariDataSource.INSTANCE.getOrInit(dbConfig);
            if (ds instanceof com.zaxxer.hikari.HikariDataSource h) {
                dbConfig = new DbConfigSnapshot(
                        dbConfig.jdbcUrl(), dbConfig.username(), dbConfig.password(), dbConfig.driverClassName(),
                        h.getMaximumPoolSize(), h.getMinimumIdle(), dbConfig.idleTimeoutMs(),
                        dbConfig.connectionTimeoutMs(), dbConfig.maxLifetimeMs());
            }

            stats = PostgresStatisticsEstimator.estimateStatistics(ds, config);
            log.info("Estimated table statistics: rowCount={}, avgRowSizeBytes={}", stats.rowCount(), stats.avgRowSizeBytes());

            int shardCount;
            int workerCount;
            String machineType = config.getMachineType() != null && !config.getMachineType().isBlank()
                    ? config.getMachineType() : "n2-standard-4";
            if (config.getShards() != null && config.getShards() > 0 && config.getWorkers() != null && config.getWorkers() > 0) {
                shardCount = config.getShards();
                workerCount = config.getWorkers();
                log.info("Using config shard plan: shards={}, workers={}, machineType={}", shardCount, workerCount, machineType);
            } else {
                workerCount = config.getWorkers() != null && config.getWorkers() > 0 ? config.getWorkers() : 1;
                shardCount = shardPlanner.suggestShardCountForCandidate(
                        machineType,
                        workerCount,
                        stats.rowCount(),
                        stats.avgRowSizeBytes(),
                        dbConfig.maximumPoolSize());
                log.info("Calculated shard plan (agent API): shards={}, workers={}, machineType={}", shardCount, workerCount, machineType);
            }

            DataSource dataSource = HikariDataSource.INSTANCE.getOrInit(dbConfig);
            
            // 5. Detect schema with robust validation
            Schema baseSchema = PostgresSchemaDetector.detectSchema(dataSource, config, metricsCollector);
            // Schema detection already logs detailed information, so we just confirm here
            log.debug("Schema detection completed: {} fields ready for processing", baseSchema.getFieldCount());
            
            // 5a. Add shard_id field to schema for tracking
            Schema schema = Schema.builder()
                    .addFields(baseSchema.getFields())
                    .addField("shard_id", Schema.FieldType.INT32)
                    .build();
            log.info("Enhanced schema with shard_id field, total fields: {}", schema.getFieldCount());
            
            // 6. Build sharded query with primary key detection
            String query = buildShardedQuery(dataSource, config);
            log.info("Built sharded query for {} shards. Query pattern: {}", shardCount, 
                    query.length() > 200 ? query.substring(0, 200) + "..." : query);
            
            // 6a. Calculate adaptive fetch size for large datasets
            // For huge partition column datasets, use larger fetch size to reduce round trips
            int baseFetchSize = config.getFetchSize() > 0 ? config.getFetchSize() : 5000;
            int fetchSize = calculateAdaptiveFetchSize(baseFetchSize, stats.rowCount(), shardCount);
            log.info("Using fetch size: {} (base: {}, rowCount: {}, shards: {})", 
                    fetchSize, baseFetchSize, stats.rowCount(), shardCount);
            
            // 7. Create shards and read data
            PCollection<Integer> shards = pipeline
                    .apply("CreateShards", org.apache.beam.sdk.transforms.Create.of(
                            IntStream.range(0, shardCount).boxed().toList()));
            
            // 8. Read data using ParDo with sharding and transaction isolation
            // When maxConcurrentShards < shardCount, load runs in phases (shift-based); semaphore limits concurrent connections.
            int maxConcurrent = (config.getMaxConcurrentShards() != null && config.getMaxConcurrentShards() > 0 && config.getMaxConcurrentShards() < shardCount)
                    ? config.getMaxConcurrentShards() : 0;
            if (maxConcurrent > 0) {
                log.info("Shift-based load: {} partitions, max {} concurrent shards (80% pool headroom)", shardCount, maxConcurrent);
            }
            PCollection<Row> rows = shards.apply("ReadFromPostgres", 
                    ParDo.of(new ReadShardDoFn(dbConfig, query, shardCount, fetchSize, schema, baseSchema, stats.rowCount(),
                            config.getQueryTimeout(), config.getSocketTimeout(), config.getStatementTimeout(), 
                            config.getEnableProgressLogging(), maxConcurrent)))
                    .setRowSchema(schema);
            
            // Record metrics
            metricsCollector.recordPostgresRead(System.currentTimeMillis() - readStartTime);
            
            return rows;
            
        } catch (Exception e) {
            metricsCollector.recordPostgresReadError();
            log.error("Failed to read data from PostgreSQL", e);
            throw new RuntimeException("Failed to read data from PostgreSQL: " + e.getMessage(), e);
        }
    }

    private void validateConfig(PipelineConfigSource config) {
        if (config == null) {
            throw new IllegalArgumentException("PipelineConfigSource cannot be null");
        }
        InputValidator.validateJdbcUrl(config.getJdbcUrl());
        if (config.getTable() != null && !config.getTable().isBlank()) {
            InputValidator.validateIdentifier(config.getTable(), "table name");
        }
        if (config.getMaximumPoolSize() > 0) {
            InputValidator.validatePoolSize(config.getMaximumPoolSize());
        }
    }

    private DbConfigSnapshot createDbConfigSnapshot(PipelineConfigSource config) {
        return createDbConfigSnapshot(config, null);
    }

    private DbConfigSnapshot createDbConfigSnapshot(PipelineConfigSource config, Integer overrideMaxPoolSize) {
        int maxPoolSize = (overrideMaxPoolSize != null && overrideMaxPoolSize > 0)
                ? overrideMaxPoolSize
                : (config.getMaximumPoolSize() > 0 ? config.getMaximumPoolSize() : config.getFallbackPoolSize());
        maxPoolSize = Math.max(4, Math.min(maxPoolSize, 100));
        int minIdle = config.getMinimumIdle() > 0
                ? Math.min(config.getMinimumIdle(), maxPoolSize)
                : Math.min(4, maxPoolSize);
        return new DbConfigSnapshot(
                config.getJdbcUrl(),
                config.getUsername(),
                config.getPassword(),
                config.getDriver() != null ? config.getDriver() : "org.postgresql.Driver",
                maxPoolSize,
                minIdle,
                config.getIdleTimeout() > 0 ? config.getIdleTimeout() : 300000L,
                config.getConnectionTimeout() > 0 ? config.getConnectionTimeout() : 300000L,
                config.getMaxLifetime() > 0 ? config.getMaxLifetime() : 1800000L
        );
    }

    // Statistics estimation moved to PostgresStatisticsEstimator
    // Schema detection moved to PostgresSchemaDetector

    // ==================== Sharded Query Building ====================
    //
    // This method decides which column(s) to use for splitting the table across shards,
    // and optionally applies a partition filter (single value or date range).
    //
    // CONFIG EXAMPLES (YAML or pipeline.config.source):
    //   # Full table load, auto-detect shard column (PK/index/partition key):
    //   table: public.orders
    //
    //   # Hash-shard on a specific column (fastest when you know the column):
    //   upperBoundColumn: id
    //
    //   # Load one partition only (e.g. one day):
    //   upperBoundColumn: created_date
    //   partitionValue: "2024-01-15"
    //
    //   # Load a date range (e.g. January 2024):
    //   upperBoundColumn: created_date
    //   partitionStartValue: "2024-01-01"
    //   partitionEndValue: "2024-01-31"
    //
    //   # Table has no PK/index: specify fallback column for ROW_NUMBER() sharding:
    //   shardColumn: updated_at
    //

    /**
     * Builds a sharded SQL query for parallel data reading.
     *
     * <p><b>Shard column priority</b> (which column is used to assign each row to a shard):
     * <ol>
     *   <li><b>upperBoundColumn</b> (config) – Hash-based; use when you want to force a specific column.
     *       Example: upperBoundColumn: id</li>
     *   <li><b>Auto-detected</b> – Index → single PK → composite PK (first column) → partition key → non-null from first 3 columns.
     *       Example: table has PRIMARY KEY (id) → we use "id" for hashing.</li>
     *   <li><b>shardColumn</b> (config) – ROW_NUMBER() over that column; stable but slower. Use when table has no PK/index.</li>
     *   <li><b>Auto-detected ordering column</b> – Same as above, column chosen from first columns.</li>
     *   <li><b>ctid</b> – Last resort; unstable if VACUUM runs.</li>
     * </ol>
     *
     * <p><b>Partition filtering</b> (optional – limits which rows are read):
     * <ul>
     *   <li><b>Single value:</b> partitionValue + partition column (upperBoundColumn or auto-detected).
     *       Example: partitionValue: "2024-01-15", upperBoundColumn: created_date
     *       → WHERE created_date = DATE '2024-01-15'</li>
     *   <li><b>Date range:</b> partitionStartValue + partitionEndValue + partition column.
     *       Example: partitionStartValue: "2024-01-01", partitionEndValue: "2024-01-31", upperBoundColumn: created_date
     *       → WHERE created_date >= DATE '2024-01-01' AND created_date <= DATE '2024-01-31'</li>
     * </ul>
     *
     * @param dataSource DataSource for database connection
     * @param config Pipeline configuration (table, upperBoundColumn, partitionValue, etc.)
     * @return SQL query string with sharding WHERE clause (and optional partition filter)
     */
    private String buildShardedQuery(DataSource dataSource, PipelineConfigSource config) {
        String tableName = config.getTable();
        if (tableName == null || tableName.isBlank()) {
            throw new IllegalArgumentException("Table name is required");
        }
        
        // Check for partition value filtering (single value or date range)
        String partitionValue = config.getPartitionValue();
        String partitionStartValue = config.getPartitionStartValue();
        String partitionEndValue = config.getPartitionEndValue();
        boolean hasPartitionRange = partitionStartValue != null && !partitionStartValue.isBlank()
                && partitionEndValue != null && !partitionEndValue.isBlank();
        boolean hasPartitionSingle = partitionValue != null && !partitionValue.isBlank();
        boolean hasPartitionFilter = hasPartitionSingle || hasPartitionRange;
        String partitionFilterDesc = hasPartitionRange ? (partitionStartValue + " to " + partitionEndValue) : partitionValue;
        
        // Log partition filtering status
        if (!hasPartitionFilter) {
            log.debug("Partition filter not provided - optional. " +
                    "System will process all data from table '{}'. " +
                    "Use partitionValue (single) or partitionStartValue + partitionEndValue (date range) with upperBoundColumn or auto-detected partition column.",
                    tableName);
        } else if (hasPartitionRange) {
            log.info("Partition range provided: '{}' to '{}' - range filter will be applied when partition column is available",
                    partitionStartValue, partitionEndValue);
        } else {
            log.info("partitionValue provided: '{}' - single-value partition filter will be applied when partition column is available",
                    partitionValue);
        }
        
        // --- Priority 1: Config-specified column (upperBoundColumn) ---
        // Example YAML: upperBoundColumn: id  → hash on "id", best when you know the best column
        String upperBoundCol = config.getUpperBoundColumn();
        if (upperBoundCol != null && !upperBoundCol.isBlank()) {
            // Validate column name to prevent SQL injection (fail fast)
            InputValidator.validateColumnName(upperBoundCol);
            
            // Log shard column selection and verify distribution potential
            if (hasPartitionFilter) {
                if (hasPartitionRange) {
                    log.info("Using upperBoundColumn '{}' for hash-based sharding with partition range filter: '{}' to '{}'",
                            upperBoundCol, partitionStartValue, partitionEndValue);
                } else {
                    log.info("Using upperBoundColumn '{}' for hash-based sharding with partition value filter: '{}'",
                            upperBoundCol, partitionValue);
                }
            } else {
                log.info("Using upperBoundColumn '{}' for hash-based sharding (fastest, most stable). " +
                        "Query will use hashtext() function for even distribution across shards.", upperBoundCol);
            }
            // Verify column has sufficient distinct values for good distribution
            verifyShardColumnDistribution(dataSource, config, upperBoundCol, tableName);
            return buildHashBasedQuery(tableName, upperBoundCol, dataSource, config);
        }
        
        // --- Priority 2: Auto-detect (index → PK → composite PK first col → partition key → non-null cols) ---
        // Example: table has PRIMARY KEY (id) → we use "id" for hash-based sharding; no config needed
        ShardColumnInfo shardColumnInfo = detectStableShardColumn(dataSource, config);
        if (shardColumnInfo != null && shardColumnInfo.columnName != null && !shardColumnInfo.columnName.isBlank()) {
            // Check if multiple columns are specified (comma-separated)
            if (shardColumnInfo.columnName.contains(",")) {
                // Multiple columns detected - use multi-column sharding
                String[] columnNames = shardColumnInfo.columnName.split(",");
                List<String> columns = new ArrayList<>();
                for (String col : columnNames) {
                    columns.add(col.trim());
                }
                if (hasPartitionFilter) {
                    log.info("Auto-detected {} stable shard column(s) {} (types: {}) for multi-column hash-based sharding with partition filter: '{}'. " +
                            "Table has no PK/index but multiple non-null columns found - using multi-column hash-based sharding.", 
                            columns.size(), columns, shardColumnInfo.columnType, partitionFilterDesc);
                } else {
                    log.info("Auto-detected {} stable shard column(s) {} (types: {}) for multi-column hash-based sharding. " +
                            "Table has no PK/index but multiple non-null columns found - using multi-column hash-based sharding.", 
                            columns.size(), columns, shardColumnInfo.columnType);
                }
                // Verify first column has sufficient distinct values for good distribution
                verifyShardColumnDistribution(dataSource, config, columns.get(0), tableName);
                String partitionFilterCol = (hasPartitionFilter && (config.getUpperBoundColumn() == null || config.getUpperBoundColumn().isBlank())) ? columns.get(0) : null;
                return buildMultiColumnHashBasedQuery(tableName, columns, dataSource, config, partitionFilterCol);
            } else {
                // Single column - use standard hash-based sharding
                if (hasPartitionFilter) {
                    log.info("Auto-detected stable shard column '{}' (type: {}) for hash-based sharding with partition filter: '{}'. " +
                            "Table has primary key/index - using fast hash-based sharding with hashtext() for even distribution.", 
                            shardColumnInfo.columnName, shardColumnInfo.columnType, partitionFilterDesc);
                } else {
                    log.info("Auto-detected stable shard column '{}' (type: {}) for hash-based sharding. " +
                            "Table has primary key/index - using fast hash-based sharding with hashtext() for even distribution.", 
                            shardColumnInfo.columnName, shardColumnInfo.columnType);
                }
                // Verify column has sufficient distinct values for good distribution
                verifyShardColumnDistribution(dataSource, config, shardColumnInfo.columnName, tableName);
                String partitionFilterCol = (hasPartitionFilter && (config.getUpperBoundColumn() == null || config.getUpperBoundColumn().isBlank())) ? shardColumnInfo.columnName : null;
                return buildHashBasedQuery(tableName, shardColumnInfo.columnName, dataSource, config, partitionFilterCol);
            }
        }
        
        // --- Priority 3: Config fallback column (shardColumn) for ROW_NUMBER() sharding ---
        // Example YAML: shardColumn: updated_at  → use when table has no PK/index; stable but slower
        String userShardColumn = config.getShardColumn();
        if (userShardColumn != null && !userShardColumn.isBlank()) {
            // Validate column name to prevent SQL injection
            InputValidator.validateColumnName(userShardColumn);
            
            // Check if the provided column is numeric or timestamp type (optimal for ordering)
            String columnDataType = PostgresPartitionValueFormatter.getColumnDataType(dataSource, config, userShardColumn);
            if (columnDataType != null && !PostgresPartitionValueFormatter.isNumericOrTimestampType(columnDataType)) {
                log.warn("shardColumn '{}' is not numeric or timestamp type (current type: '{}'). " +
                        "For optimal performance, consider providing a numeric column (bigint, integer, numeric) " +
                        "or timestamp column (timestamp, date) either via command line " +
                        "(--pipeline.config.source.shardColumn=column_name) or in pipeline_config.yml (shardColumn: column_name). " +
                        "Current column will work but may be slower for ROW_NUMBER() ordering.",
                        userShardColumn, columnDataType);
            }
            
            if (hasPartitionFilter) {
                log.info("Applied ROW_NUMBER() (shardColumn: '{}') with partition filter: '{}'. " +
                        "Table has no stable keys (PK/index) - using user-specified column for stable sharding.", 
                        userShardColumn, partitionFilterDesc);
            } else {
                log.info("Applied ROW_NUMBER() (shardColumn: '{}'): stable but slower. " +
                        "Table has no stable keys (PK/index) - using user-specified column for stable sharding.", 
                        userShardColumn);
            }
            return buildRowNumberBasedQuery(tableName, userShardColumn, dataSource, config);
        }
        
        // --- Priority 4: Auto-detected ordering column for ROW_NUMBER() (no PK/index, no shardColumn set) ---
        OrderingColumnInfo orderingColumnInfo = detectOrderingColumn(dataSource, config);
        if (orderingColumnInfo != null && orderingColumnInfo.columnName != null && !orderingColumnInfo.columnName.isBlank()) {
            String orderingColumn = orderingColumnInfo.columnName;
            String dataType = orderingColumnInfo.dataType;
            
            // Check if the auto-detected column is optimal (numeric or timestamp)
            boolean isOptimal = dataType != null && PostgresPartitionValueFormatter.isNumericOrTimestampType(dataType);
            String typeInfo = dataType != null ? String.format("type: '%s'", dataType) : "type: unknown";
            
            if (!isOptimal) {
                if (hasPartitionFilter) {
                    log.warn("Applied ROW_NUMBER() (auto-detected ordering column: '{}', {}) with partition filter: '{}'. " +
                            "Auto-detected column is not numeric or timestamp type. " +
                            "No stable shard column found for table '{}' and shardColumn not provided in config. " +
                            "For optimal performance, consider adding a primary key, index, or partition key, " +
                            "or specifying a numeric/timestamp shardColumn in config (shardColumn: column_name) or via command line " +
                            "(--pipeline.config.source.shardColumn=column_name).",
                            orderingColumn, typeInfo, partitionFilterDesc, tableName);
                } else {
                    log.warn("Applied ROW_NUMBER() (auto-detected ordering column: '{}', {}): stable but slower. " +
                            "Auto-detected column is not numeric or timestamp type. " +
                            "No stable shard column found for table '{}' and shardColumn not provided in config. " +
                            "For optimal performance, consider adding a primary key, index, or partition key, " +
                            "or specifying a numeric/timestamp shardColumn in config (shardColumn: column_name) or via command line " +
                            "(--pipeline.config.source.shardColumn=column_name).",
                            orderingColumn, typeInfo, tableName);
                }
            } else {
                if (hasPartitionFilter) {
                    log.warn("Applied ROW_NUMBER() (auto-detected ordering column: '{}', {}) with partition filter: '{}'. " +
                            "No stable shard column found for table '{}' and shardColumn not provided in config. " +
                            "Consider adding a primary key, index, or partition key, or specifying shardColumn in config for better performance.",
                            orderingColumn, typeInfo, partitionFilterDesc, tableName);
                } else {
                    log.warn("Applied ROW_NUMBER() (auto-detected ordering column: '{}', {}): stable but slower. " +
                            "No stable shard column found for table '{}' and shardColumn not provided in config. " +
                            "Consider adding a primary key, index, or partition key, or specifying shardColumn in config for better performance.",
                            orderingColumn, typeInfo, tableName);
                }
            }
            return buildRowNumberBasedQuery(tableName, orderingColumn, dataSource, config);
        }
        
        // --- Priority 5: Last resort – ctid (unstable; avoid if possible) ---
        if (hasPartitionFilter) {
            log.error("No columns available for ordering in table '{}'. Using unstable ctid for sharding with partition filter: '{}'. " +
                    "WARNING: ctid-based sharding is UNSTABLE even with no data changes because: " +
                    "(1) VACUUM operations can change ctid values, (2) rows may be skipped or duplicated across shards. " +
                    "Strongly recommend adding a primary key, index, or at least one column for stable ordering.", 
                    tableName, partitionFilterDesc);
        } else {
            log.error("No columns available for ordering in table '{}'. Using unstable ctid for sharding. " +
                    "WARNING: ctid-based sharding is UNSTABLE even with no data changes because: " +
                    "(1) VACUUM operations can change ctid values, (2) rows may be skipped or duplicated across shards. " +
                    "Strongly recommend adding a primary key, index, or at least one column for stable ordering.", tableName);
        }
        return buildCtidBasedQuery(tableName, dataSource, config);
    }
    
    /**
     * Record to hold shard column information including type.
     */
    private record ShardColumnInfo(String columnName, String columnType) {}
    
    /**
     * Represents multiple columns to be used together for sharding.
     * Used when 2 or 3 non-null columns are available from the first 3 columns.
     */
    private record MultiShardColumnInfo(List<String> columnNames, List<String> columnTypes) {
        public int getColumnCount() {
            return columnNames != null ? columnNames.size() : 0;
        }
    }
    
    /**
     * Record to hold ordering column information including data type.
     */
    private record OrderingColumnInfo(String columnName, String dataType) {}
    
    /**
     * Detects a stable shard column for a table (used when upperBoundColumn is not set in config).
     *
     * <p><b>Priority order</b> (first match wins):
     * <ol>
     *   <li><b>Index key</b> – Any non-PK index (unique or not). Example: table has INDEX(tenant_id) → use "tenant_id".</li>
     *   <li><b>Primary key (single)</b> – One-column PK. Example: PRIMARY KEY (id) → use "id".</li>
     *   <li><b>Composite primary key</b> – Use first column only (faster than hashing all). Example: PRIMARY KEY (tenant_id, id) → use "tenant_id".</li>
     *   <li><b>Partition key</b> – Table is partitioned; use partition column. Example: PARTITION BY RANGE (created_date) → use "created_date".</li>
     *   <li><b>Non-null from first 3 columns</b> – No PK/index; use 2–3 non-null columns for multi-column hash, or 1 as fallback.</li>
     * </ol>
     *
     * <p>When both PK and index exist, index is chosen (priority 1). For composite PK we use only the first column for performance.
     *
     * @param dataSource DataSource for database connection
     * @param config Pipeline configuration (table name used to resolve schema.table)
     * @return ShardColumnInfo with column name and type, or null if none found
     */
    private ShardColumnInfo detectStableShardColumn(DataSource dataSource, PipelineConfigSource config) {
        try (Connection conn = dataSource.getConnection()) {
            String tableName = config.getTable();
            if (tableName == null || tableName.isBlank()) {
                return null;
            }
            
            // Parse schema and table name
            String[] parts = tableName.split("\\.");
            String schemaName = parts.length > 1 ? parts[0] : "public";
            String tableNameOnly = parts.length > 1 ? parts[1] : parts[0];
            
            // Priority 1: Check for index key (unique or non-unique index)
            ShardColumnInfo indexColumn = detectIndexKey(conn, schemaName, tableNameOnly);
            if (indexColumn != null) {
                return indexColumn;
            }
            
            // Priority 2 & 3: Check for primary key (single or composite; use first column only for composite for performance)
            List<String> pkColumns = detectPrimaryKeyColumnList(conn, schemaName, tableNameOnly);
            if (pkColumns != null && !pkColumns.isEmpty()) {
                String column = pkColumns.get(0);
                InputValidator.validateColumnName(column);
                String columnType = pkColumns.size() == 1 ? "PRIMARY_KEY" : "COMPOSITE_PRIMARY_KEY_FIRST";
                return new ShardColumnInfo(column, columnType);
            }
            
            // Priority 4: Check for partition key
            ShardColumnInfo partitionKey = detectPartitionKey(conn, schemaName, tableNameOnly);
            if (partitionKey != null) {
                return partitionKey;
            }
            
            // Priority 5: Check for non-null columns from first 3 columns
            // This helps when no PK/index exists but first columns have no null values
            // Prefer using multiple columns (3 if available, 2 as minimum) for better distribution
            MultiShardColumnInfo multiColumnInfo = detectMultipleNonNullColumnsFromFirstThree(conn, schemaName, tableNameOnly);
            if (multiColumnInfo != null && multiColumnInfo.getColumnCount() >= 2) {
                log.info("Selected {} non-null column(s) {} from first 3 columns for multi-column sharding (no PK/index found).", 
                        multiColumnInfo.getColumnCount(), multiColumnInfo.columnNames);
                // Return the first column info for backward compatibility, but we'll use all columns in the query
                return new ShardColumnInfo(
                    String.join(",", multiColumnInfo.columnNames), 
                    String.join(",", multiColumnInfo.columnTypes)
                );
            }
            
            // Fallback to single column if less than 2 non-null columns found
            ShardColumnInfo nonNullColumn = detectNonNullColumnFromFirstThree(conn, schemaName, tableNameOnly);
            if (nonNullColumn != null) {
                log.info("Selected non-null column '{}' from first 3 columns for sharding (no PK/index found).", 
                        nonNullColumn.columnName);
                return nonNullColumn;
            }
            
            return null; // No stable column found
            
        } catch (SQLException e) {
            log.warn("Failed to detect stable shard column for table '{}': {}. Will fall back to ctid.", 
                    config.getTable(), e.getMessage());
            return null;
        } catch (IllegalArgumentException e) {
            log.warn("Invalid shard column name detected: {}. Will fall back to ctid.", e.getMessage());
            return null;
        }
    }
    
    /**
     * Detects multiple non-null columns from the first 3 columns for sharding.
     * Priority: Use 3 columns if all are non-null, otherwise use 2 columns if minimum available.
     * Checks actual data to verify columns have no null values.
     * 
     * @param conn Database connection
     * @param schemaName Schema name
     * @param tableNameOnly Table name (without schema)
     * @return MultiShardColumnInfo with 2-3 columns, or null if less than 2 non-null columns found
     */
    private MultiShardColumnInfo detectMultipleNonNullColumnsFromFirstThree(Connection conn, String schemaName, String tableNameOnly) throws SQLException {
        // Get first 3 columns ordered by position
        String sql = "SELECT column_name, data_type, ordinal_position " +
                    "FROM information_schema.columns " +
                    "WHERE table_schema = ? " +
                    "  AND table_name = ? " +
                    "  AND column_name NOT LIKE '\\_%' " + // Exclude system columns
                    "ORDER BY ordinal_position " +
                    "LIMIT 3";
        
        List<String> firstThreeColumns = new ArrayList<>();
        Map<String, String> columnTypes = new HashMap<>();
        
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, schemaName);
            ps.setString(2, tableNameOnly);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String columnName = rs.getString("column_name");
                    String dataType = rs.getString("data_type");
                    firstThreeColumns.add(columnName);
                    columnTypes.put(columnName, dataType);
                }
            }
        }
        
        if (firstThreeColumns.isEmpty()) {
            return null;
        }
        
        // Check which of the first 3 columns have no null values in actual data
        List<String> nonNullColumns = PostgresDataQualityChecker.checkNonNullColumnsInData(conn, schemaName, tableNameOnly, firstThreeColumns);
        
        if (nonNullColumns.size() >= 3) {
            // All 3 columns are non-null - use all 3
            List<String> types = new ArrayList<>();
            for (String col : firstThreeColumns) {
                types.add(columnTypes.getOrDefault(col, "unknown"));
            }
            log.info("All 3 columns are non-null. Using all 3 columns {} for multi-column sharding.", firstThreeColumns);
            return new MultiShardColumnInfo(new ArrayList<>(firstThreeColumns), types);
        } else if (nonNullColumns.size() >= 2) {
            // At least 2 columns are non-null - use the first 2 non-null columns
            List<String> selectedColumns = new ArrayList<>(nonNullColumns.subList(0, 2));
            List<String> types = new ArrayList<>();
            for (String col : selectedColumns) {
                types.add(columnTypes.getOrDefault(col, "unknown"));
            }
            log.info("Found {} non-null columns. Using first 2 columns {} for multi-column sharding.", 
                    nonNullColumns.size(), selectedColumns);
            return new MultiShardColumnInfo(selectedColumns, types);
        }
        
        // Less than 2 non-null columns found
        return null;
    }
    
    /**
     * Detects a single non-null column from the first 3 columns for sharding (fallback).
     * Checks actual data to verify columns have no null values.
     * Prefers numeric/timestamp columns for optimal sharding performance.
     * 
     * @param conn Database connection
     * @param schemaName Schema name
     * @param tableNameOnly Table name (without schema)
     * @return ShardColumnInfo with column name and type, or null if none found
     */
    private ShardColumnInfo detectNonNullColumnFromFirstThree(Connection conn, String schemaName, String tableNameOnly) throws SQLException {
        // Get first 3 columns ordered by position
        String sql = "SELECT column_name, data_type, ordinal_position " +
                    "FROM information_schema.columns " +
                    "WHERE table_schema = ? " +
                    "  AND table_name = ? " +
                    "  AND column_name NOT LIKE '\\_%' " + // Exclude system columns
                    "ORDER BY ordinal_position " +
                    "LIMIT 3";
        
        List<String> firstThreeColumns = new ArrayList<>();
        Map<String, String> columnTypes = new HashMap<>();
        
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, schemaName);
            ps.setString(2, tableNameOnly);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String columnName = rs.getString("column_name");
                    String dataType = rs.getString("data_type");
                    firstThreeColumns.add(columnName);
                    columnTypes.put(columnName, dataType);
                }
            }
        }
        
        if (firstThreeColumns.isEmpty()) {
            return null;
        }
        
        // Check which of the first 3 columns have no null values in actual data
        List<String> nonNullColumns = PostgresDataQualityChecker.checkNonNullColumnsInData(conn, schemaName, tableNameOnly, firstThreeColumns);
        
        if (nonNullColumns.isEmpty()) {
            log.debug("No non-null columns found among first 3 columns in table '{}'", tableNameOnly);
            return null;
        }
        
        // Prefer numeric/timestamp columns from non-null columns
        for (String columnName : nonNullColumns) {
                        String dataType = columnTypes.get(columnName);
                        if (dataType != null && PostgresPartitionValueFormatter.isNumericOrTimestampType(dataType)) {
                log.debug("Selected non-null numeric/timestamp column '{}' (type: {}) from first 3 columns for sharding", 
                        columnName, dataType);
                return new ShardColumnInfo(columnName, dataType);
            }
        }
        
        // If no numeric/timestamp found, use first non-null column
        String firstNonNullColumn = nonNullColumns.get(0);
        String dataType = columnTypes.get(firstNonNullColumn);
        log.debug("Selected non-null column '{}' (type: {}) from first 3 columns for sharding", 
                firstNonNullColumn, dataType);
        return new ShardColumnInfo(firstNonNullColumn, dataType != null ? dataType : "unknown");
    }
    
    /**
     * Detects an index column for sharding (excluding PK indexes).
     * Prefers unique index, then any index; returns first column of chosen index.
     * Example: table has INDEX(tenant_id), UNIQUE(booking_id) → returns first column of unique index if any, else tenant_id.
     */
    private ShardColumnInfo detectIndexKey(Connection conn, String schemaName, String tableNameOnly) throws SQLException {
        // Query to find index columns
        // Prefer unique indexes, then non-unique indexes
        // Use pg_indexes and pg_index system catalogs
        String sql = "SELECT a.attname AS column_name, " +
                    "       CASE WHEN i.indisunique THEN 'UNIQUE_INDEX' ELSE 'INDEX' END AS index_type " +
                    "FROM pg_index i " +
                    "JOIN pg_class c ON c.oid = i.indrelid " +
                    "JOIN pg_namespace n ON n.oid = c.relnamespace " +
                    "JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(i.indkey) " +
                    "WHERE n.nspname = ? " +
                    "  AND c.relname = ? " +
                    "  AND i.indisprimary = false " + // Exclude primary key indexes (handled separately)
                    "  AND a.attnum > 0 " + // Exclude system columns
                    "  AND NOT a.attisdropped " + // Exclude dropped columns
                    "ORDER BY i.indisunique DESC, " + // Prefer unique indexes
                    "         array_position(i.indkey, a.attnum) " + // First column of index
                    "LIMIT 1";
        
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, schemaName);
            ps.setString(2, tableNameOnly);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    String columnName = rs.getString("column_name");
                    String indexType = rs.getString("index_type");
                    InputValidator.validateColumnName(columnName);
                    return new ShardColumnInfo(columnName, indexType);
                }
            }
        }
        return null;
    }
    
    /**
     * Returns all primary key column names in order. Single PK → [id]; composite PK → [tenant_id, id].
     * Used to pick first column for sharding when composite (for performance we use one column only).
     */
    private List<String> detectPrimaryKeyColumnList(Connection conn, String schemaName, String tableNameOnly) throws SQLException {
        String sql = "SELECT kcu.column_name " +
                    "FROM information_schema.table_constraints tc " +
                    "JOIN information_schema.key_column_usage kcu " +
                    "  ON tc.constraint_name = kcu.constraint_name " +
                    "  AND tc.table_schema = kcu.table_schema " +
                    "  AND tc.table_catalog = kcu.table_catalog " +
                    "WHERE tc.constraint_type = 'PRIMARY KEY' " +
                    "  AND tc.table_schema = ? " +
                    "  AND tc.table_name = ? " +
                    "ORDER BY kcu.ordinal_position";
        List<String> columns = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, schemaName);
            ps.setString(2, tableNameOnly);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    columns.add(rs.getString("column_name"));
                }
            }
        }
        return columns.isEmpty() ? null : columns;
    }

    /**
     * Detects primary key column(s) for a table.
     * 
     * @param conn Database connection
     * @param schemaName Schema name
     * @param tableNameOnly Table name (without schema)
     * @param allowComposite If true, allows composite primary keys; if false, only single-column PKs
     * @return ShardColumnInfo with primary key column name, or null if no primary key found
     */
    private ShardColumnInfo detectPrimaryKey(Connection conn, String schemaName, String tableNameOnly, boolean allowComposite) throws SQLException {
        List<String> pkColumns = detectPrimaryKeyColumnList(conn, schemaName, tableNameOnly);
        if (pkColumns == null || pkColumns.isEmpty()) {
            return null;
        }
        if (!allowComposite && pkColumns.size() > 1) {
            return null;
        }
        String columnName = pkColumns.get(0);
        InputValidator.validateColumnName(columnName);
        String columnType = pkColumns.size() == 1 ? "PRIMARY_KEY" : "COMPOSITE_PRIMARY_KEY_FIRST";
        return new ShardColumnInfo(columnName, columnType);
    }
    
    /**
     * Detects partition key column for a partitioned table (e.g. PARTITION BY RANGE (created_date) → "created_date").
     * Returns first partition column; used for hash-based sharding and for partition filter when user sets partitionValue/range.
     */
    private ShardColumnInfo detectPartitionKey(Connection conn, String schemaName, String tableNameOnly) throws SQLException {
        // pg_partitioned_table + pg_attribute to get partition column(s); we use the first one
        String sql = "SELECT a.attname AS column_name " +
                    "FROM pg_partitioned_table pt " +
                    "JOIN pg_class c ON c.oid = pt.partrelid " +
                    "JOIN pg_namespace n ON n.oid = c.relnamespace " +
                    "JOIN pg_attribute a ON a.attrelid = c.oid " +
                    "  AND a.attnum = ANY(pt.partattrs) " +
                    "WHERE n.nspname = ? " +
                    "  AND c.relname = ? " +
                    "  AND a.attnum > 0 " +
                    "  AND NOT a.attisdropped " +
                    "ORDER BY array_position(pt.partattrs, a.attnum) " + // First partition column
                    "LIMIT 1";
        
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, schemaName);
            ps.setString(2, tableNameOnly);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    String columnName = rs.getString("column_name");
                    InputValidator.validateColumnName(columnName);
                    return new ShardColumnInfo(columnName, "PARTITION_KEY");
                }
            }
        }
        return null;
    }
    
    /**
     * Builds a hash-based sharding query using a single column.
     * Each row is assigned to a shard by: hash(column_value) % shardCount == shardIndex.
     * Example: shardColumn="id" → WHERE (abs(...md5(id::text)...) % ?) = ?  (plus optional partition filter).
     *
     * @param tableName Table name (e.g. public.orders)
     * @param shardColumn Column used for hashing (e.g. id, created_date)
     * @param dataSource Used to resolve partition column type when partition filter is applied
     * @param config May contain partitionValue or partitionStartValue/partitionEndValue for WHERE filter
     * @return SQL SELECT with WHERE (hash % ?) = ? and optional AND partition_filter
     */
    private String buildHashBasedQuery(String tableName, String shardColumn, DataSource dataSource, PipelineConfigSource config) {
        return buildHashBasedQuery(tableName, shardColumn, dataSource, config, null);
    }

    private String buildHashBasedQuery(String tableName, String shardColumn, DataSource dataSource, PipelineConfigSource config, String partitionFilterColumn) {
        // Validate column name to prevent SQL injection
        InputValidator.validateColumnName(shardColumn);
        
        // Build partition value filter if specified (use partitionFilterColumn when auto-detected and partitionValue set)
        String partitionFilter = buildPartitionFilter(dataSource, config, partitionFilterColumn);
        
        // PostgreSQL hash-based sharding query
        // Uses MD5 hash converted to bigint for better distribution across all data types
        // The MD5 approach provides more uniform distribution than hashtext() for low-cardinality columns
        // ABS() ensures positive modulo result for proper shard distribution [0, shardCount-1]
        // Using substr(md5(...),1,8) to get first 8 hex chars, convert to bit(32), then to bigint
        // The bitwise AND with 2147483647 (0x7FFFFFFF) ensures positive 31-bit value
        String baseQuery = "SELECT * FROM " + tableName + " " +
               "WHERE (abs((('x'||substr(md5(COALESCE(" + shardColumn + "::text, '')),1,8))::bit(32))::bigint & 2147483647) % ?) = ?";
        
        log.info("Hash-based sharding query for column '{}': using MD5 hash with ABS() for distribution. " +
                "NULL values will be treated as empty string.", shardColumn);
        
        // Append partition date filter if provided
        if (partitionFilter != null && !partitionFilter.isBlank()) {
            return baseQuery + " AND " + partitionFilter;
        }
        
        return baseQuery;
    }
    
    /**
     * Builds a hash-based sharding query using 2 or 3 columns (no PK/index case).
     * Hashes concat(col1, '|', col2, ...) for shard assignment. Example: columns [a, b] → hash(a||'|'||b) % ? = ?.
     *
     * @param tableName Table name (e.g. public.events)
     * @param shardColumns Typically 2–3 non-null columns from first columns (no PK/index)
     * @param dataSource For partition filter column type resolution
     * @param config Optional partition single value or range
     * @return SQL SELECT with WHERE (hash on concatenated columns % ?) = ? and optional partition filter
     */
    private String buildMultiColumnHashBasedQuery(String tableName, List<String> shardColumns, DataSource dataSource, PipelineConfigSource config) {
        return buildMultiColumnHashBasedQuery(tableName, shardColumns, dataSource, config, null);
    }

    private String buildMultiColumnHashBasedQuery(String tableName, List<String> shardColumns, DataSource dataSource, PipelineConfigSource config, String partitionFilterColumn) {
        // Validate all column names to prevent SQL injection
        for (String column : shardColumns) {
            InputValidator.validateColumnName(column);
        }
        
        // Build partition value filter if specified (use partitionFilterColumn when auto-detected and partitionValue set)
        String partitionFilter = buildPartitionFilter(dataSource, config, partitionFilterColumn);
        
        // Concatenate columns with a delimiter for hashing
        // Use COALESCE to handle NULLs (treat as empty string)
        // Concatenate with '||' separator to create a composite key
        StringBuilder columnExpression = new StringBuilder();
        for (int i = 0; i < shardColumns.size(); i++) {
            if (i > 0) {
                columnExpression.append("||'|'||"); // Use '|' as delimiter between columns
            }
            columnExpression.append("COALESCE(").append(shardColumns.get(i)).append("::text, '')");
        }
        
        // PostgreSQL hash-based sharding query using multiple columns
        // Concatenates all column values and hashes the result
        // ABS() ensures positive modulo result for proper shard distribution [0, shardCount-1]
        String baseQuery = "SELECT * FROM " + tableName + " " +
               "WHERE (abs((('x'||substr(md5(" + columnExpression.toString() + "),1,8))::bit(32))::bigint & 2147483647) % ?) = ?";
        
        log.info("Multi-column hash-based sharding query for {} column(s) {}: using MD5 hash with ABS() for distribution. " +
                "NULL values will be treated as empty string.", shardColumns.size(), shardColumns);
        
        // Append partition date filter if provided
        if (partitionFilter != null && !partitionFilter.isBlank()) {
            return baseQuery + " AND " + partitionFilter;
        }
        
        return baseQuery;
    }
    
    /**
     * Detects a column suitable for stable ordering when no primary key/index is available.
     * 
     * Priority order:
     * 1. Non-null columns from first 3 columns (prefer numeric/timestamp types)
     * 2. Any non-null column (prefer numeric/timestamp types)
     * 3. Any column (prefer numeric/timestamp types)
     * 
     * Note: Sharding uses ONE column at a time. This method selects the best single column.
     * 
     * @param dataSource DataSource for database connection
     * @param config Pipeline configuration
     * @return OrderingColumnInfo with column name and data type, or null if no suitable column found
     */
    private OrderingColumnInfo detectOrderingColumn(DataSource dataSource, PipelineConfigSource config) {
        try (Connection conn = dataSource.getConnection()) {
            String tableName = config.getTable();
            if (tableName == null || tableName.isBlank()) {
                return null;
            }
            
            // Parse schema and table name
            String[] parts = tableName.split("\\.");
            String schemaName = parts.length > 1 ? parts[0] : "public";
            String tableNameOnly = parts.length > 1 ? parts[1] : parts[0];
            
            // Priority 1: Check for non-null columns from first 3 columns
            // Get first 3 columns
            String firstThreeSql = "SELECT column_name, data_type, ordinal_position " +
                        "FROM information_schema.columns " +
                        "WHERE table_schema = ? " +
                        "  AND table_name = ? " +
                        "  AND column_name NOT LIKE '\\_%' " + // Exclude system columns
                        "ORDER BY ordinal_position " +
                        "LIMIT 3";
            
            List<String> firstThreeColumns = new ArrayList<>();
            Map<String, String> firstThreeTypes = new HashMap<>();
            
            try (PreparedStatement ps = conn.prepareStatement(firstThreeSql)) {
                ps.setString(1, schemaName);
                ps.setString(2, tableNameOnly);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String columnName = rs.getString("column_name");
                        String dataType = rs.getString("data_type");
                        firstThreeColumns.add(columnName);
                        firstThreeTypes.put(columnName, dataType);
                    }
                }
            }
            
            // Check which of first 3 have no null values
            if (!firstThreeColumns.isEmpty()) {
                List<String> nonNullInFirstThree = PostgresDataQualityChecker.checkNonNullColumnsInData(conn, schemaName, tableNameOnly, firstThreeColumns);
                
                if (!nonNullInFirstThree.isEmpty()) {
                    // Prefer numeric/timestamp from non-null first 3 columns
                    for (String columnName : nonNullInFirstThree) {
                        String dataType = firstThreeTypes.get(columnName);
                        if (dataType != null && PostgresPartitionValueFormatter.isNumericOrTimestampType(dataType)) {
                            log.info("Selected non-null column '{}' (type: {}) from first 3 columns for ROW_NUMBER() sharding", 
                                    columnName, dataType);
                            return new OrderingColumnInfo(columnName, dataType);
                        }
                    }
                    
                    // If no numeric/timestamp, use first non-null column
                    String firstNonNull = nonNullInFirstThree.get(0);
                    String dataType = firstThreeTypes.get(firstNonNull);
                    log.info("Selected non-null column '{}' (type: {}) from first 3 columns for ROW_NUMBER() sharding", 
                            firstNonNull, dataType);
                    return new OrderingColumnInfo(firstNonNull, dataType != null ? dataType : "unknown");
                }
            }
            
            // Priority 2: Query to find any suitable column for ordering
            // Prefer numeric types (bigint, integer, numeric), then timestamp types, then any other
            // Prefer non-nullable columns (is_nullable = 'NO')
            String sql = "SELECT column_name, data_type, is_nullable " +
                        "FROM information_schema.columns " +
                        "WHERE table_schema = ? " +
                        "  AND table_name = ? " +
                        "  AND column_name NOT LIKE '\\_%' " + // Exclude system columns (starting with _)
                        "ORDER BY " +
                        "  CASE WHEN is_nullable = 'NO' THEN 0 ELSE 1 END, " + // Prefer non-nullable
                        "  CASE " +
                        "    WHEN data_type IN ('bigint', 'integer', 'int', 'smallint', 'numeric', 'decimal', 'real', 'double precision') THEN 1 " +
                        "    WHEN data_type IN ('timestamp without time zone', 'timestamp with time zone', 'date', 'time') THEN 2 " +
                        "    ELSE 3 " +
                        "  END, " +
                        "  ordinal_position " +
                        "LIMIT 1";
            
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, schemaName);
                ps.setString(2, tableNameOnly);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        String columnName = rs.getString("column_name");
                        String dataType = rs.getString("data_type");
                        String isNullable = rs.getString("is_nullable");
                        InputValidator.validateColumnName(columnName);
                        
                        boolean isNonNull = "NO".equalsIgnoreCase(isNullable);
                        if (isNonNull) {
                            log.debug("Selected non-nullable column '{}' (type: {}) for ROW_NUMBER() sharding", 
                                    columnName, dataType);
                        }
                        return new OrderingColumnInfo(columnName, dataType);
                    }
                }
            }
            
            return null; // No suitable column found
            
        } catch (SQLException e) {
            log.warn("Failed to detect ordering column for table '{}': {}. Will fall back to ctid.", 
                    config.getTable(), e.getMessage());
            return null;
        } catch (IllegalArgumentException e) {
            log.warn("Invalid ordering column name detected: {}. Will fall back to ctid.", e.getMessage());
            return null;
        }
    }
    
    // getColumnDataType and isNumericOrTimestampType moved to PostgresPartitionValueFormatter
    
    /**
     * Builds a ROW_NUMBER()-based sharding query using a stable ordering column.
     * This approach is more stable than ctid because:
     * - Uses logical column values instead of physical row locations
     * - Not affected by VACUUM operations
     * - Provides consistent sharding even if data doesn't change
     * 
     * Note: This approach may be slower than hash-based sharding for large tables
     * because it requires a window function calculation.
     * 
     * @param tableName Table name (may include schema)
     * @param orderingColumn Column name to use for ordering
     * @param dataSource DataSource for database connection (for partition value filtering)
     * @param config Pipeline configuration (for partition value filtering)
     * @return SQL query with ROW_NUMBER()-based WHERE clause (and partition value filter if specified)
     */
    private String buildRowNumberBasedQuery(String tableName, String orderingColumn, DataSource dataSource, PipelineConfigSource config) {
        // Validate column name to prevent SQL injection
        InputValidator.validateColumnName(orderingColumn);
        
        // Build partition value filter if specified
        String partitionFilter = buildPartitionFilter(dataSource, config);
        
        // Use ROW_NUMBER() with stable ordering for sharding
        // This ensures each row gets a consistent row number based on the ordering column
        // The modulo operation distributes rows evenly across shards
        String baseQuery = "SELECT * FROM (" +
               "  SELECT *, ROW_NUMBER() OVER (ORDER BY " + orderingColumn + ") - 1 AS rn " +
               "  FROM " + tableName;
        
        // Add partition value filter to inner query if provided
        if (partitionFilter != null && !partitionFilter.isBlank()) {
            baseQuery += " WHERE " + partitionFilter;
        }
        
        baseQuery += ") t WHERE (rn % ?) = ?";
        
        return baseQuery;
    }
    
    /**
     * Builds a ctid-based sharding query as last resort.
     * Uses PostgreSQL's physical row identifier (ctid) for sharding.
     * 
     * ⚠️ WARNING: ctid is UNSTABLE even with no data changes because:
     * - VACUUM operations can change ctid values for unchanged rows
     * - REPEATABLE_READ isolation does NOT prevent VACUUM from changing physical locations
     * - Rows may be skipped or duplicated across shards if VACUUM runs during read
     * - This can cause data inconsistency even in read-only scenarios
     * 
     * Only use this as an absolute last resort when no columns are available for ordering.
     * 
     * @param tableName Table name (may include schema)
     * @param dataSource DataSource for database connection (for partition value filtering)
     * @param config Pipeline configuration (for partition value filtering)
     * @return SQL query with ctid-based WHERE clause (and partition value filter if specified)
     */
    private String buildCtidBasedQuery(String tableName, DataSource dataSource, PipelineConfigSource config) {
        // Build partition value filter if specified
        String partitionFilter = buildPartitionFilter(dataSource, config);
        
        // Use PostgreSQL hashtext function on ctid for sharding
        // Note: This is unstable and should be avoided if possible
        String baseQuery = "SELECT * FROM " + tableName + " WHERE (abs(hashtext(ctid::text)) % ?) = ?";
        
        // Append partition date filter if provided
        if (partitionFilter != null && !partitionFilter.isBlank()) {
            return baseQuery + " AND " + partitionFilter;
        }
        
        return baseQuery;
    }
    
    /**
     * Verifies that the shard column has sufficient distinct values for good distribution.
     * Logs a warning if the column has very low cardinality which could cause poor shard distribution.
     * 
     * @param dataSource DataSource for database connection
     * @param config Pipeline configuration
     * @param shardColumn Column name being used for sharding
     * @param tableName Full table name (may include schema)
     */
    private void verifyShardColumnDistribution(DataSource dataSource, PipelineConfigSource config, 
                                               String shardColumn, String tableName) {
        try (Connection conn = dataSource.getConnection()) {
            // Parse schema and table name
            String[] parts = tableName.split("\\.");
            String schemaName = parts.length > 1 ? parts[0] : "public";
            String tableNameOnly = parts.length > 1 ? parts[1] : parts[0];
            
            // Get total row count
            String countQuery = "SELECT COUNT(*) AS total_rows FROM \"" + schemaName + "\".\"" + tableNameOnly + "\"";
            long totalRows = 0;
            try (PreparedStatement ps = conn.prepareStatement(countQuery);
                 ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    totalRows = rs.getLong("total_rows");
                }
            }
            
            // Get distinct count for shard column
            InputValidator.validateColumnName(shardColumn);
            String distinctQuery = "SELECT COUNT(DISTINCT " + shardColumn + ") AS distinct_count FROM \"" + 
                                 schemaName + "\".\"" + tableNameOnly + "\"";
            long distinctCount = 0;
            try (PreparedStatement ps = conn.prepareStatement(distinctQuery);
                 ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    distinctCount = rs.getLong("distinct_count");
                }
            }
            
            // Log distribution metrics
            if (totalRows > 0) {
                double distinctRatio = (double) distinctCount / totalRows;
                log.info("Shard column '{}' distribution: {} distinct values out of {} total rows (ratio: {:.2%})", 
                        shardColumn, distinctCount, totalRows, distinctRatio);
                
                // Warn if cardinality is very low (could cause poor distribution)
                if (distinctCount < 10 && totalRows > 100) {
                    log.warn("Shard column '{}' has very low cardinality ({} distinct values). " +
                            "This may cause poor distribution across shards. Consider using a different column.", 
                            shardColumn, distinctCount);
                } else if (distinctRatio < 0.1 && totalRows > 1000) {
                    log.warn("Shard column '{}' has low distinct ratio ({:.2%}). " +
                            "This may cause uneven distribution across shards.", 
                            shardColumn, distinctRatio);
                }
            }
        } catch (Exception e) {
            log.debug("Could not verify shard column distribution for '{}': {}", shardColumn, e.getMessage());
            // Don't fail the operation if verification fails
        }
    }
    
    /**
     * Builds partition filter for WHERE clause. Delegates to overload with no column override.
     */
    private String buildPartitionFilter(DataSource dataSource, PipelineConfigSource config) {
        return buildPartitionFilter(dataSource, config, null);
    }

    /**
     * Builds a partition filter fragment for WHERE conditions (single value or range).
     *
     * <p><b>Single partition value</b> – load one partition only:
     * <ul>
     *   <li>Config: partitionValue: "2024-01-15", upperBoundColumn: created_date (or auto-detected partition column)</li>
     *   <li>Result: {@code created_date = DATE '2024-01-15'}</li>
     * </ul>
     *
     * <p><b>Partition range</b> – load a date/numeric range:
     * <ul>
     *   <li>Config: partitionStartValue: "2024-01-01", partitionEndValue: "2024-01-31", upperBoundColumn: created_date</li>
     *   <li>Result: {@code created_date >= DATE '2024-01-01' AND created_date <= DATE '2024-01-31'}</li>
     * </ul>
     *
     * <p>Partition column is taken from config (upperBoundColumn) or from overridePartitionColumn when we
     * auto-detected the shard column (e.g. partition key) and user did not set upperBoundColumn.
     * Column type is detected from the schema and values are formatted (date, timestamp, integer, string).
     *
     * @param dataSource DataSource for database connection (to detect column type)
     * @param config Pipeline configuration (partitionValue, partitionStartValue, partitionEndValue, upperBoundColumn)
     * @param overridePartitionColumn When non-null, use this as partition column (e.g. auto-detected partition key)
     * @return SQL WHERE clause fragment (e.g. "col = value" or "col >= x AND col <= y"), or null if no filter
     */
    private String buildPartitionFilter(DataSource dataSource, PipelineConfigSource config, String overridePartitionColumn) {
        String partitionValue = config.getPartitionValue();
        String partitionStartValue = config.getPartitionStartValue();
        String partitionEndValue = config.getPartitionEndValue();
        boolean hasRange = partitionStartValue != null && !partitionStartValue.isBlank()
                && partitionEndValue != null && !partitionEndValue.isBlank();
        boolean hasSingle = partitionValue != null && !partitionValue.isBlank();

        if (!hasSingle && !hasRange) {
            log.debug("Partition filter not provided - skipping. All data from the table will be processed.");
            return null;
        }

        String partitionColumn = (overridePartitionColumn != null && !overridePartitionColumn.isBlank())
                ? overridePartitionColumn
                : config.getUpperBoundColumn();

        if (partitionColumn == null || partitionColumn.isBlank()) {
            log.warn("Partition value/range provided but neither upperBoundColumn nor auto-detected partition column is available. " +
                    "Skipping partition filter.");
            return null;
        }

        InputValidator.validateColumnName(partitionColumn);
        String columnDataType = PostgresPartitionValueFormatter.getColumnDataType(dataSource, config, partitionColumn);

        if (hasRange) {
            // Date (or numeric) range: column >= start AND column <= end
            String formattedStart = columnDataType != null && !columnDataType.isBlank()
                    ? PostgresPartitionValueFormatter.formatPartitionValueByType(partitionColumn, partitionStartValue, columnDataType)
                    : null;
            String formattedEnd = columnDataType != null && !columnDataType.isBlank()
                    ? PostgresPartitionValueFormatter.formatPartitionValueByType(partitionColumn, partitionEndValue, columnDataType)
                    : null;
            if (formattedStart != null && formattedEnd != null) {
                log.info("Applying partition range filter: {} >= {} AND {} <= {} (column type: '{}', range: '{}' to '{}')",
                        partitionColumn, formattedStart, partitionColumn, formattedEnd, columnDataType, partitionStartValue, partitionEndValue);
                return partitionColumn + " >= " + formattedStart + " AND " + partitionColumn + " <= " + formattedEnd;
            }
            // Fallback string range
            String escStart = partitionStartValue.replace("'", "''");
            String escEnd = partitionEndValue.replace("'", "''");
            log.warn("Could not format partition range for column type '{}'. Using string format.", columnDataType);
            return partitionColumn + " >= '" + escStart + "' AND " + partitionColumn + " <= '" + escEnd + "'";
        }

        // Single value: column = value
        if (columnDataType == null || columnDataType.isBlank()) {
            String escapedValue = partitionValue.replace("'", "''");
            log.info("Applying partition value filter: {} = '{}' (as string)", partitionColumn, partitionValue);
            return partitionColumn + " = '" + escapedValue + "'";
        }
        String formattedFilter = PostgresPartitionValueFormatter.formatPartitionValueByType(partitionColumn, partitionValue, columnDataType);
        if (formattedFilter != null) {
            log.info("Applying partition value filter: {} = {} (column type: '{}', value: '{}')",
                    partitionColumn, formattedFilter, columnDataType, partitionValue);
            return partitionColumn + " = " + formattedFilter;
        }
        String escapedValue = partitionValue.replace("'", "''");
        log.warn("Could not format partition value '{}' for column type '{}'. Using string format as fallback.", partitionValue, columnDataType);
        return partitionColumn + " = '" + escapedValue + "'";
    }
    
    // formatPartitionValueByType moved to PostgresPartitionValueFormatter

    
    /**
     * Calculates adaptive fetch size from configurable thresholds (streamnova.adaptive-fetch).
     * When row count exceeds a threshold, fetch size is increased to reduce round trips.
     *
     * <p>Large dataset: rowCount &gt; largeDatasetThresholdRows → min(largeDatasetMaxFetchSize, base * largeDatasetMultiplier).
     * <p>Medium dataset: rowCount &gt; mediumDatasetThresholdRows → min(mediumDatasetMaxFetchSize, base * mediumDatasetMultiplier).
     *
     * @param baseFetchSize Base fetch size from pipeline config (or default 5000)
     * @param rowCount Estimated table row count
     * @param shardCount Number of shards (currently unused; reserved for future per-shard tuning)
     * @return Fetch size to use for JDBC read
     */
    private int calculateAdaptiveFetchSize(int baseFetchSize, long rowCount, int shardCount) {
        long largeThreshold = adaptiveFetchProperties.getLargeDatasetThresholdRows();
        int largeMax = adaptiveFetchProperties.getLargeDatasetMaxFetchSize();
        int largeMultiplier = adaptiveFetchProperties.getLargeDatasetMultiplier();
        long mediumThreshold = adaptiveFetchProperties.getMediumDatasetThresholdRows();
        int mediumMax = adaptiveFetchProperties.getMediumDatasetMaxFetchSize();
        int mediumMultiplier = adaptiveFetchProperties.getMediumDatasetMultiplier();

        if (rowCount > largeThreshold) {
            int adaptiveSize = Math.min(largeMax, baseFetchSize * largeMultiplier);
            log.info("Large dataset detected ({} rows > {}). Using adaptive fetch size: {} (base: {}, max: {}, multiplier: {})",
                    rowCount, largeThreshold, adaptiveSize, baseFetchSize, largeMax, largeMultiplier);
            return adaptiveSize;
        }
        if (rowCount > mediumThreshold) {
            int adaptiveSize = Math.min(mediumMax, baseFetchSize * mediumMultiplier);
            log.debug("Medium-large dataset detected ({} rows > {}). Using adaptive fetch size: {} (base: {}, max: {}, multiplier: {})",
                    rowCount, mediumThreshold, adaptiveSize, baseFetchSize, mediumMax, mediumMultiplier);
            return adaptiveSize;
        }
        return baseFetchSize;
    }
    
    /**
     * Serializable DoFn for reading data from PostgreSQL shards.
     * This is a static class to avoid serialization issues with outer class references.
     * 
     * Features:
     * - Transaction isolation (REPEATABLE_READ) for snapshot consistency
     * - Explicit transaction boundaries for data consistency
     * - Row count tracking per shard for validation
     */
    private static class ReadShardDoFn extends DoFn<Integer, Row> {
        private static final ConcurrentHashMap<String, Semaphore> CONCURRENCY_SEMAPHORES = new ConcurrentHashMap<>();

        private final DbConfigSnapshot dbConfig;
        private final String query;
        private final int shardCount;
        private final int fetchSize;
        private final Schema schema; // Full schema with shard_id
        private final Schema baseSchema; // Original schema without shard_id
        private final long expectedRowCount; // Expected total row count for validation
        private final Integer queryTimeout; // Query timeout in seconds
        private final Integer socketTimeout; // Socket timeout in seconds
        private final Integer statementTimeout; // Statement timeout in seconds (PostgreSQL)
        private final Boolean enableProgressLogging; // Enable progress logging
        /** When > 0, max concurrent DB connections (shift-based load). 0 = no limit. */
        private final int maxConcurrentShards;

        // Transient fields that will be initialized in @Setup
        private transient DataSource dataSource;
        private transient org.slf4j.Logger logger;
        
        ReadShardDoFn(DbConfigSnapshot dbConfig, String query, int shardCount, int fetchSize,
                     Schema schema, Schema baseSchema, long expectedRowCount,
                     Integer queryTimeout, Integer socketTimeout, Integer statementTimeout, Boolean enableProgressLogging,
                     int maxConcurrentShards) {
            this.dbConfig = dbConfig;
            this.query = query;
            this.shardCount = shardCount;
            this.fetchSize = fetchSize;
            this.schema = schema;
            this.baseSchema = baseSchema;
            this.expectedRowCount = expectedRowCount;
            this.queryTimeout = queryTimeout;
            this.socketTimeout = socketTimeout;
            this.statementTimeout = statementTimeout;
            this.enableProgressLogging = enableProgressLogging != null ? enableProgressLogging : true;
            this.maxConcurrentShards = maxConcurrentShards <= 0 ? 0 : maxConcurrentShards;
        }
        
        @Setup
        public void setup() {
            // Initialize DataSource in @Setup to avoid serialization issues
            this.dataSource = HikariDataSource.INSTANCE.getOrInit(dbConfig);
            this.logger = org.slf4j.LoggerFactory.getLogger(ReadShardDoFn.class);
        }
        
        /**
         * Processes a single shard with transaction isolation for data consistency.
         * 
         * Uses REPEATABLE_READ isolation level to ensure all shards see the same
         * snapshot of data, preventing inconsistencies if data changes during read.
         * 
         * @param shardId the shard ID to process
         * @param out output receiver for rows
         */
        @ProcessElement
        public void processElement(@Element Integer shardId, OutputReceiver<Row> out) {
            Semaphore semaphore = null;
            if (maxConcurrentShards > 0) {
                String key = dbConfig.jdbcUrl() + "_" + maxConcurrentShards;
                semaphore = CONCURRENCY_SEMAPHORES.computeIfAbsent(key, k -> new Semaphore(maxConcurrentShards));
                semaphore.acquireUninterruptibly();
            }
            Connection conn = null;
            try {
                conn = dataSource.getConnection();
                
                // Set transaction isolation level for snapshot consistency
                // REPEATABLE_READ ensures all shards see the same point-in-time snapshot
                conn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
                conn.setAutoCommit(false); // Start explicit transaction
                
                // Set timeouts for large partition column datasets
                if (socketTimeout != null && socketTimeout > 0) {
                    // Set socket timeout via JDBC URL parameter (if not already set)
                    // Note: This is typically set at connection level, but we log it here
                    logger.debug("Socket timeout configured: {} seconds", socketTimeout);
                }
                
                // Set PostgreSQL statement timeout if configured
                if (statementTimeout != null && statementTimeout > 0) {
                    try (java.sql.Statement stmt = conn.createStatement()) {
                        stmt.execute("SET statement_timeout = " + (statementTimeout * 1000)); // Convert to milliseconds
                        logger.debug("PostgreSQL statement_timeout set to {} seconds for shard {}", statementTimeout, shardId);
                    }
                }
                
                try (PreparedStatement ps = conn.prepareStatement(query)) {
                    // Set query timeout if configured
                    if (queryTimeout != null && queryTimeout > 0) {
                        ps.setQueryTimeout(queryTimeout);
                        logger.debug("Query timeout set to {} seconds for shard {}", queryTimeout, shardId);
                    }
                    
                    ps.setInt(1, shardCount); // Total shard count for modulo
                    ps.setInt(2, shardId);    // This shard ID (must be in range [0, shardCount-1])
                    ps.setFetchSize(fetchSize);
                    
                    logger.debug("Full query for shardId={}: {}", shardId, query);
                    
                    // Validate shardId is in correct range
                    if (shardId < 0 || shardId >= shardCount) {
                        logger.error("Invalid shardId={} for shardCount={}. ShardId must be in range [0, {}). " +
                                "This will cause incorrect shard distribution!", shardId, shardCount, shardCount);
                    }
                    
                    int rowCount = 0;
                    long lastProgressLog = System.currentTimeMillis();
                    int progressLogInterval = enableProgressLogging ? 30000 : Integer.MAX_VALUE; // Log every 30s if enabled
                    
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            Row row = mapResultSetToRow(rs, shardId);
                            out.output(row);
                            rowCount++;
                            
                            // Progress logging for large datasets
                            if (enableProgressLogging && (System.currentTimeMillis() - lastProgressLog) > progressLogInterval) {
                                logger.info("Shard {} progress: read {} rows so far...", shardId, rowCount);
                                lastProgressLog = System.currentTimeMillis();
                            }
                        }
                    }
                    
                    // Log shard completion with row count
                    if (enableProgressLogging) {
                        logger.info("Shard {} completed: read {} rows", shardId, rowCount);
                    } else {
                        logger.debug("Shard {} completed: read {} rows", shardId, rowCount);
                    }
                    
                    // Commit read-only transaction (ensures consistency)
                    conn.commit();
                } catch (SQLException e) {
                    // Rollback on error
                    if (conn != null && !conn.isClosed()) {
                        try {
                            conn.rollback();
                        } catch (SQLException rollbackEx) {
                            logger.error("Failed to rollback transaction for shard {}", shardId, rollbackEx);
                        }
                    }
                    throw new RuntimeException("Failed to read shard " + shardId, e);
                }
                
            } catch (SQLException e) {
                logger.error("Database error while reading shard {}", shardId, e);
                throw new RuntimeException("Failed to read shard " + shardId + ": " + e.getMessage(), e);
            } finally {
                if (semaphore != null) {
                    semaphore.release();
                }
                // Ensure connection is closed
                if (conn != null) {
                    try {
                        if (!conn.isClosed()) {
                            conn.close();
                        }
                    } catch (SQLException e) {
                        logger.warn("Error closing connection for shard {}", shardId, e);
                    }
                }
            }
        }
        
        /**
         * Maps a JDBC ResultSet row to an Apache Beam Row.
         * Uses TypeConverter utility to handle all type conversions for PostgreSQL types,
         * including BYTEA, JSON, UUID, arrays, and large objects (BLOB/CLOB).
         * 
         * @param rs the ResultSet positioned at the current row
         * @param shardId the shard ID to include in the row
         * @return Apache Beam Row with all fields converted to schema-compatible types
         * @throws SQLException if database access error occurs
         */
        private Row mapResultSetToRow(ResultSet rs, int shardId) throws SQLException {
            List<Object> values = new ArrayList<>();
            
            // First, add all base schema fields from ResultSet
            for (int i = 0; i < baseSchema.getFieldCount(); i++) {
                Schema.Field field = baseSchema.getField(i);
                Object value = rs.getObject(i + 1);
                
                // Use TypeConverter utility to convert value to schema-compatible type
                // TypeConverter handles all PostgreSQL-specific types
                try {
                    value = TypeConverter.convertToSchemaType(value, field, field.getName());
                } catch (IllegalArgumentException e) {
                    logger.error("Failed to convert value for field '{}' (type: {}, value: {}): {}", 
                            field.getName(), 
                            value != null ? value.getClass().getName() : "null",
                            value,
                            e.getMessage());
                    throw new RuntimeException(
                            String.format("Type conversion failed for field '%s' (schema type: %s): %s", 
                                    field.getName(), 
                                    field.getType().getTypeName(),
                                    e.getMessage()), e);
                }
                
                values.add(value);
            }
            
            // Add shard_id as the last field (INT32)
            values.add(shardId);
            
            return Row.withSchema(schema).addValues(values).build();
        }
    }
}
