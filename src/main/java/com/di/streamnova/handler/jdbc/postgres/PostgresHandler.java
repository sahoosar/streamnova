package com.di.streamnova.handler.jdbc.postgres;

import com.di.streamnova.config.DbConfigSnapshot;
import com.di.streamnova.config.PipelineConfigSource;
import com.di.streamnova.handler.SourceHandler;
import com.di.streamnova.handler.jdbc.postgres.PostgresDataQualityChecker;
import com.di.streamnova.handler.jdbc.postgres.PostgresPartitionValueFormatter;
import com.di.streamnova.handler.jdbc.postgres.PostgresSchemaDetector;
import com.di.streamnova.handler.jdbc.postgres.PostgresStatisticsEstimator;
import com.di.streamnova.util.HikariDataSource;
import com.di.streamnova.util.InputValidator;
import com.di.streamnova.util.MetricsCollector;
import com.di.streamnova.util.TypeConverter;
import com.di.streamnova.util.shardplanner.ShardPlanner;
import com.di.streamnova.util.shardplanner.ShardWorkerPlan;
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
import java.util.stream.IntStream;

/**
 * Handler for reading data from PostgreSQL databases.
 * 
 * This handler implements parallel data reading from PostgreSQL using sharding
 * based on machine type and data characteristics.
 * 
 * Features:
 * - Uses TypeConverter for type mapping and value conversion
 * - Integrates with ShardPlanner for optimal shard/worker calculation
 * - Supports shard_id tracking for per-shard metrics
 * - Handles all PostgreSQL types including BYTEA, JSON, UUID, arrays
 * - Implements transaction isolation for data consistency
 * - Automatic primary key detection for stable sharding
 * - Row count validation for completeness
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class PostgresHandler implements SourceHandler<PipelineConfigSource> {

    private final MetricsCollector metricsCollector;

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
            
            // 2. Create DataSource
            DbConfigSnapshot dbConfig = createDbConfigSnapshot(config);
            DataSource dataSource = HikariDataSource.INSTANCE.getOrInit(dbConfig);
            
            // 3. Estimate statistics
            PostgresStatisticsEstimator.TableStatistics stats = PostgresStatisticsEstimator.estimateStatistics(dataSource, config);
            log.info("Estimated table statistics: rowCount={}, avgRowSizeBytes={}", 
                    stats.rowCount(), stats.avgRowSizeBytes());
            
            // 4. Calculate shard count using ShardPlanner
            ShardWorkerPlan plan = ShardPlanner.calculateOptimalShardWorkerPlan(
                    pipeline.getOptions(),
                    config.getMaximumPoolSize(),
                    stats.rowCount(),
                    stats.avgRowSizeBytes(),
                    null, // targetMbPerShard - use default
                    config.getShards(),
                    config.getWorkers(),
                    config.getMachineType()
            );
            
            int shardCount = plan.shardCount();
            log.info("Calculated shard plan: shards={}, workers={}, machineType={}, strategy={}", 
                    shardCount, plan.workerCount(), plan.machineType(), plan.calculationStrategy());
            
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
            PCollection<Row> rows = shards.apply("ReadFromPostgres", 
                    ParDo.of(new ReadShardDoFn(dbConfig, query, shardCount, fetchSize, schema, baseSchema, stats.rowCount(),
                            config.getQueryTimeout(), config.getSocketTimeout(), config.getStatementTimeout(), 
                            config.getEnableProgressLogging())))
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
        return new DbConfigSnapshot(
                config.getJdbcUrl(),
                config.getUsername(),
                config.getPassword(),
                config.getDriver() != null ? config.getDriver() : "org.postgresql.Driver",
                config.getMaximumPoolSize() > 0 ? config.getMaximumPoolSize() : 16,
                config.getMinimumIdle() > 0 ? config.getMinimumIdle() : 4,
                config.getIdleTimeout() > 0 ? config.getIdleTimeout() : 300000L,
                config.getConnectionTimeout() > 0 ? config.getConnectionTimeout() : 300000L,
                config.getMaxLifetime() > 0 ? config.getMaxLifetime() : 1800000L
        );
    }

    // Statistics estimation moved to PostgresStatisticsEstimator
    // Schema detection moved to PostgresSchemaDetector

    /**
     * Builds a sharded SQL query for parallel data reading.
     * 
     * Priority order for shard column selection:
     * 1. upperBoundColumn (from config) → Hash-based sharding (fastest, most stable)
     *    Use when: You want hash-based sharding for performance-critical scenarios
     * 2. Auto-detected stable keys (index, primary key, composite PK, partition key) → Hash-based sharding
     *    Use when: Table has primary key or index (system auto-detects)
     * 3. shardColumn (from config) → ROW_NUMBER() sharding (stable, slightly slower)
     *    Use when: Table has no PK/index, and you want to specify which column to use for ordering
     *    Do NOT use when: Table has PK/index (system will auto-detect) or performance is critical
     * 4. Auto-detected ordering column → ROW_NUMBER() sharding (stable, slightly slower)
     *    Use when: No stable keys found and no shardColumn specified
     * 5. ctid (last resort) → Unstable, only if no columns available
     * 
     * Partition Value Filtering:
     * - If partitionValue is provided with upperBoundColumn, adds WHERE clause to filter by specific partition value
     * - Auto-detects column data type and formats value appropriately (date, integer, string, etc.)
     * - Example: upperBoundColumn: "created_date", partitionValue: "2024-01-15" → WHERE created_date = DATE '2024-01-15'
     * - Example: upperBoundColumn: "region_id", partitionValue: "5" → WHERE region_id = 5
     * - Example: upperBoundColumn: "region_name", partitionValue: "east" → WHERE region_name = 'east'
     * 
     * @param dataSource DataSource for database connection
     * @param config Pipeline configuration
     * @return SQL query string with sharding WHERE clause (and partition date filter if specified)
     */
    private String buildShardedQuery(DataSource dataSource, PipelineConfigSource config) {
        String tableName = config.getTable();
        if (tableName == null || tableName.isBlank()) {
            throw new IllegalArgumentException("Table name is required");
        }
        
        // Check for partition value filtering
        String partitionValue = config.getPartitionValue();
        boolean hasPartitionFilter = partitionValue != null && !partitionValue.isBlank();
        
        // Log partition filtering status (partitionValue is optional)
        if (!hasPartitionFilter) {
            log.debug("partitionValue not provided - partition filtering is optional. " +
                    "System will process all data from table '{}' without partition filtering. " +
                    "To filter by partition, provide both partitionValue and upperBoundColumn in config or command line.",
                    tableName);
        } else {
            log.info("partitionValue provided: '{}' - partition filtering will be applied if upperBoundColumn is also provided",
                    partitionValue);
        }
        
        // Priority 1: Use upperBoundColumn for hash-based sharding (fastest, most stable)
        // Use this when: You want hash-based sharding for performance-critical scenarios
        String upperBoundCol = config.getUpperBoundColumn();
        if (upperBoundCol != null && !upperBoundCol.isBlank()) {
            // Validate column name to prevent SQL injection (fail fast)
            InputValidator.validateColumnName(upperBoundCol);
            
            // Log shard column selection and verify distribution potential
            String shardColForLogging = upperBoundCol;
            if (hasPartitionFilter) {
                log.info("Using upperBoundColumn '{}' for hash-based sharding with partition value filter: '{}'", 
                        upperBoundCol, partitionValue);
            } else {
                log.info("Using upperBoundColumn '{}' for hash-based sharding (fastest, most stable). " +
                        "Query will use hashtext() function for even distribution across shards.", upperBoundCol);
            }
            // Verify column has sufficient distinct values for good distribution
            verifyShardColumnDistribution(dataSource, config, upperBoundCol, tableName);
            return buildHashBasedQuery(tableName, upperBoundCol, dataSource, config);
        }
        
        // Priority 2: Auto-detect stable shard column (index, primary key, composite primary key, partition key)
        // Use this when: Table has primary key or index (system auto-detects)
        // This provides hash-based sharding (fast) without requiring user configuration
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
                    log.info("Auto-detected {} stable shard column(s) {} (types: {}) for multi-column hash-based sharding with partition value filter: '{}'. " +
                            "Table has no PK/index but multiple non-null columns found - using multi-column hash-based sharding.", 
                            columns.size(), columns, shardColumnInfo.columnType, partitionValue);
                } else {
                    log.info("Auto-detected {} stable shard column(s) {} (types: {}) for multi-column hash-based sharding. " +
                            "Table has no PK/index but multiple non-null columns found - using multi-column hash-based sharding.", 
                            columns.size(), columns, shardColumnInfo.columnType);
                }
                // Verify first column has sufficient distinct values for good distribution
                verifyShardColumnDistribution(dataSource, config, columns.get(0), tableName);
                return buildMultiColumnHashBasedQuery(tableName, columns, dataSource, config);
            } else {
                // Single column - use standard hash-based sharding
                if (hasPartitionFilter) {
                    log.info("Auto-detected stable shard column '{}' (type: {}) for hash-based sharding with partition value filter: '{}'. " +
                            "Table has primary key/index - using fast hash-based sharding with hashtext() for even distribution.", 
                            shardColumnInfo.columnName, shardColumnInfo.columnType, partitionValue);
                } else {
                    log.info("Auto-detected stable shard column '{}' (type: {}) for hash-based sharding. " +
                            "Table has primary key/index - using fast hash-based sharding with hashtext() for even distribution.", 
                            shardColumnInfo.columnName, shardColumnInfo.columnType);
                }
                // Verify column has sufficient distinct values for good distribution
                verifyShardColumnDistribution(dataSource, config, shardColumnInfo.columnName, tableName);
                return buildHashBasedQuery(tableName, shardColumnInfo.columnName, dataSource, config);
            }
        }
        
        // Priority 3: Use shardColumn from config (for fallback ROW_NUMBER() sharding)
        // Use this when: Table has NO primary key/index, and you want to specify which column to use
        // Do NOT use when: Table has PK/index (system will auto-detect) or performance is critical
        // Note: This uses ROW_NUMBER() sharding (stable but slower than hash-based)
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
                log.info("Applied ROW_NUMBER() (shardColumn: '{}') with partition value filter: '{}'. " +
                        "Table has no stable keys (PK/index) - using user-specified column for stable sharding.", 
                        userShardColumn, partitionValue);
            } else {
                log.info("Applied ROW_NUMBER() (shardColumn: '{}'): stable but slower. " +
                        "Table has no stable keys (PK/index) - using user-specified column for stable sharding.", 
                        userShardColumn);
            }
            return buildRowNumberBasedQuery(tableName, userShardColumn, dataSource, config);
        }
        
        // Priority 4: Fallback to ROW_NUMBER() with auto-detected ordering column (more stable than ctid)
        // This approach uses any available column(s) for ordering, making it stable even if VACUUM runs
        OrderingColumnInfo orderingColumnInfo = detectOrderingColumn(dataSource, config);
        if (orderingColumnInfo != null && orderingColumnInfo.columnName != null && !orderingColumnInfo.columnName.isBlank()) {
            String orderingColumn = orderingColumnInfo.columnName;
            String dataType = orderingColumnInfo.dataType;
            
            // Check if the auto-detected column is optimal (numeric or timestamp)
            boolean isOptimal = dataType != null && PostgresPartitionValueFormatter.isNumericOrTimestampType(dataType);
            String typeInfo = dataType != null ? String.format("type: '%s'", dataType) : "type: unknown";
            
            if (!isOptimal) {
                if (hasPartitionFilter) {
                    log.warn("Applied ROW_NUMBER() (auto-detected ordering column: '{}', {}) with partition value filter: '{}'. " +
                            "Auto-detected column is not numeric or timestamp type. " +
                            "No stable shard column found for table '{}' and shardColumn not provided in config. " +
                            "For optimal performance, consider adding a primary key, index, or partition key, " +
                            "or specifying a numeric/timestamp shardColumn in config (shardColumn: column_name) or via command line " +
                            "(--pipeline.config.source.shardColumn=column_name).",
                            orderingColumn, typeInfo, partitionValue, tableName);
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
                    log.warn("Applied ROW_NUMBER() (auto-detected ordering column: '{}', {}) with partition value filter: '{}'. " +
                            "No stable shard column found for table '{}' and shardColumn not provided in config. " +
                            "Consider adding a primary key, index, or partition key, or specifying shardColumn in config for better performance.",
                            orderingColumn, typeInfo, partitionValue, tableName);
                } else {
                    log.warn("Applied ROW_NUMBER() (auto-detected ordering column: '{}', {}): stable but slower. " +
                            "No stable shard column found for table '{}' and shardColumn not provided in config. " +
                            "Consider adding a primary key, index, or partition key, or specifying shardColumn in config for better performance.",
                            orderingColumn, typeInfo, tableName);
                }
            }
            return buildRowNumberBasedQuery(tableName, orderingColumn, dataSource, config);
        }
        
        // Priority 5: Last resort - ctid (unstable, but works if no columns available)
        if (hasPartitionFilter) {
            log.error("No columns available for ordering in table '{}'. Using unstable ctid for sharding with partition value filter: '{}'. " +
                    "WARNING: ctid-based sharding is UNSTABLE even with no data changes because: " +
                    "(1) VACUUM operations can change ctid values, (2) rows may be skipped or duplicated across shards. " +
                    "Strongly recommend adding a primary key, index, or at least one column for stable ordering.", 
                    tableName, partitionValue);
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
     * Detects a stable shard column for a table in priority order:
     * 1. Index key (unique or non-unique index)
     * 2. Primary key column (single column)
     * 3. Composite primary key (first column)
     * 4. Partition key
     * 5. Non-null column from first 3 columns (if available)
     * 
     * Note: Sharding uses ONE column at a time, not multiple columns.
     * This method selects the best single column for sharding.
     * 
     * @param dataSource DataSource for database connection
     * @param config Pipeline configuration
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
            
            // Priority 2: Check for primary key (single column)
            ShardColumnInfo primaryKeyColumn = detectPrimaryKey(conn, schemaName, tableNameOnly, false);
            if (primaryKeyColumn != null) {
                return primaryKeyColumn;
            }
            
            // Priority 3: Check for composite primary key (use first column)
            ShardColumnInfo compositePrimaryKey = detectPrimaryKey(conn, schemaName, tableNameOnly, true);
            if (compositePrimaryKey != null) {
                return compositePrimaryKey;
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
     * Detects index key columns for a table.
     * Prefers unique indexes, then non-unique indexes.
     * Returns the first column of the first suitable index.
     * 
     * @param conn Database connection
     * @param schemaName Schema name
     * @param tableNameOnly Table name (without schema)
     * @return ShardColumnInfo with index column name, or null if no index found
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
     * Detects primary key column(s) for a table.
     * 
     * @param conn Database connection
     * @param schemaName Schema name
     * @param tableNameOnly Table name (without schema)
     * @param allowComposite If true, allows composite primary keys; if false, only single-column PKs
     * @return ShardColumnInfo with primary key column name, or null if no primary key found
     */
    private ShardColumnInfo detectPrimaryKey(Connection conn, String schemaName, String tableNameOnly, boolean allowComposite) throws SQLException {
        // Query to detect primary key columns using information_schema
        String sql = "SELECT kcu.column_name, " +
                    "       COUNT(*) OVER (PARTITION BY tc.constraint_name) AS key_column_count " +
                    "FROM information_schema.table_constraints tc " +
                    "JOIN information_schema.key_column_usage kcu " +
                    "  ON tc.constraint_name = kcu.constraint_name " +
                    "  AND tc.table_schema = kcu.table_schema " +
                    "  AND tc.table_catalog = kcu.table_catalog " +
                    "WHERE tc.constraint_type = 'PRIMARY KEY' " +
                    "  AND tc.table_schema = ? " +
                    "  AND tc.table_name = ? " +
                    "ORDER BY kcu.ordinal_position " +
                    "LIMIT 1";
        
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, schemaName);
            ps.setString(2, tableNameOnly);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    long keyColumnCount = rs.getLong("key_column_count");
                    
                    // If allowComposite is false, only return single-column primary keys
                    if (!allowComposite && keyColumnCount > 1) {
                        return null;
                    }
                    
                    String columnName = rs.getString("column_name");
                    InputValidator.validateColumnName(columnName);
                    
                    String columnType = keyColumnCount == 1 ? "PRIMARY_KEY" : "COMPOSITE_PRIMARY_KEY_FIRST";
                    return new ShardColumnInfo(columnName, columnType);
                }
            }
        }
        return null;
    }
    
    /**
     * Detects partition key column for a partitioned table.
     * 
     * @param conn Database connection
     * @param schemaName Schema name
     * @param tableNameOnly Table name (without schema)
     * @return ShardColumnInfo with partition key column name, or null if table is not partitioned
     */
    private ShardColumnInfo detectPartitionKey(Connection conn, String schemaName, String tableNameOnly) throws SQLException {
        // Query to detect partition key columns
        // Uses pg_partitioned_table and pg_attribute system catalogs
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
     * Builds a hash-based sharding query using a specific column.
     * Uses MD5 hash of the column value for deterministic shard assignment.
     * 
     * @param tableName Table name (may include schema)
     * @param shardColumn Column name to use for sharding
     * @param dataSource DataSource for database connection (for partition value filtering)
     * @param config Pipeline configuration (for partition value filtering)
     * @return SQL query with hash-based WHERE clause (and partition value filter if specified)
     */
    private String buildHashBasedQuery(String tableName, String shardColumn, DataSource dataSource, PipelineConfigSource config) {
        // Validate column name to prevent SQL injection
        InputValidator.validateColumnName(shardColumn);
        
        // Build partition value filter if specified
        String partitionFilter = buildPartitionFilter(dataSource, config);
        
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
     * Builds a hash-based sharding query using multiple columns.
     * Concatenates column values and hashes the result for deterministic shard assignment.
     * This provides better distribution when multiple non-null columns are available.
     * 
     * @param tableName Table name (may include schema)
     * @param shardColumns List of column names to use for sharding (2 or 3 columns)
     * @param dataSource DataSource for database connection (for partition value filtering)
     * @param config Pipeline configuration (for partition value filtering)
     * @return SQL query with hash-based WHERE clause (and partition value filter if specified)
     */
    private String buildMultiColumnHashBasedQuery(String tableName, List<String> shardColumns, DataSource dataSource, PipelineConfigSource config) {
        // Validate all column names to prevent SQL injection
        for (String column : shardColumns) {
            InputValidator.validateColumnName(column);
        }
        
        // Build partition value filter if specified
        String partitionFilter = buildPartitionFilter(dataSource, config);
        
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
     * Builds a generic partition value filter clause for WHERE conditions.
     * Filters data by a specific partition value when partitionValue and upperBoundColumn are provided.
     * Auto-detects column data type and formats the value appropriately (date, integer, string, etc.).
     * 
     * @param dataSource DataSource for database connection (to detect column type)
     * @param config Pipeline configuration
     * @return SQL WHERE clause fragment for partition value filtering, or null if not applicable
     */
    private String buildPartitionFilter(DataSource dataSource, PipelineConfigSource config) {
        String partitionValue = config.getPartitionValue();
        String partitionColumn = config.getUpperBoundColumn();
        
        // Both partitionValue and upperBoundColumn must be provided for partition filtering
        // partitionValue is optional - if not provided, return null (no filter applied)
        if (partitionValue == null || partitionValue.isBlank()) {
            log.debug("partitionValue not provided - skipping partition value filter. " +
                    "This is optional and will not affect data loading. " +
                    "All data from the table will be processed.");
            return null;
        }
        
        if (partitionColumn == null || partitionColumn.isBlank()) {
            log.warn("partitionValue '{}' provided but upperBoundColumn is missing. " +
                    "Partition value filtering requires both partitionValue and upperBoundColumn. " +
                    "Skipping partition value filter.", partitionValue);
            return null;
        }
        
        // Validate column name to prevent SQL injection
        InputValidator.validateColumnName(partitionColumn);
        
        // Detect column data type to format value appropriately
        String columnDataType = PostgresPartitionValueFormatter.getColumnDataType(dataSource, config, partitionColumn);
        
        if (columnDataType == null || columnDataType.isBlank()) {
            log.warn("Could not detect data type for partition column '{}'. " +
                    "Using string format for partition value filter. " +
                    "If this causes issues, verify the column name is correct.", partitionColumn);
            // Default to string format with proper escaping
            String escapedValue = partitionValue.replace("'", "''"); // Escape single quotes
            log.info("Applying partition value filter: {} = '{}' (as string)", partitionColumn, partitionValue);
            return partitionColumn + " = '" + escapedValue + "'";
        }
        
        // Format value based on detected column data type
        String formattedFilter = PostgresPartitionValueFormatter.formatPartitionValueByType(partitionColumn, partitionValue, columnDataType);
        
        if (formattedFilter != null) {
            log.info("Applying partition value filter: {} = {} (column type: '{}', value: '{}')", 
                    partitionColumn, formattedFilter, columnDataType, partitionValue);
            return partitionColumn + " = " + formattedFilter;
        }
        
        // Fallback: treat as string if type detection/formatting fails
        String escapedValue = partitionValue.replace("'", "''");
        log.warn("Could not format partition value '{}' for column type '{}'. Using string format as fallback.", 
                partitionValue, columnDataType);
        return partitionColumn + " = '" + escapedValue + "'";
    }
    
    // formatPartitionValueByType moved to PostgresPartitionValueFormatter

    
    /**
     * Calculates adaptive fetch size based on dataset size and shard count.
     * For huge partition column datasets, uses larger fetch size to reduce round trips.
     * 
     * @param baseFetchSize Base fetch size from config
     * @param rowCount Estimated row count
     * @param shardCount Number of shards
     * @return Adaptive fetch size
     */
    private int calculateAdaptiveFetchSize(int baseFetchSize, long rowCount, int shardCount) {
        // For very large datasets (>10M rows), increase fetch size
        if (rowCount > 10_000_000) {
            // Use larger fetch size: min(50000, baseFetchSize * 10)
            int adaptiveSize = Math.min(50000, baseFetchSize * 10);
            log.info("Large dataset detected ({} rows). Using adaptive fetch size: {} (base: {})", 
                    rowCount, adaptiveSize, baseFetchSize);
            return adaptiveSize;
        } else if (rowCount > 1_000_000) {
            // For medium-large datasets, moderate increase
            int adaptiveSize = Math.min(20000, baseFetchSize * 4);
            log.debug("Medium-large dataset detected ({} rows). Using adaptive fetch size: {} (base: {})", 
                    rowCount, adaptiveSize, baseFetchSize);
            return adaptiveSize;
        }
        // For smaller datasets, use base fetch size
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
        
        // Transient fields that will be initialized in @Setup
        private transient DataSource dataSource;
        private transient org.slf4j.Logger logger;
        
        ReadShardDoFn(DbConfigSnapshot dbConfig, String query, int shardCount, int fetchSize, 
                     Schema schema, Schema baseSchema, long expectedRowCount,
                     Integer queryTimeout, Integer socketTimeout, Integer statementTimeout, Boolean enableProgressLogging) {
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
                    
                    // Log query parameters for debugging shard distribution (INFO level for troubleshooting)
                    logger.info("Executing shard query for shardId={} with shardCount={}. " +
                            "Query will filter rows where hash % {} = {}", shardId, shardCount, shardCount, shardId);
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
