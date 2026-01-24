package com.di.streamnova.handler.jdbc;

import com.di.streamnova.config.DbConfigSnapshot;
import com.di.streamnova.config.PipelineConfigSource;
import com.di.streamnova.handler.SourceHandler;
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
import java.util.List;
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
            TableStatistics stats = estimateStatistics(dataSource, config);
            log.info("Estimated table statistics: rowCount={}, avgRowSizeBytes={}", 
                    stats.rowCount, stats.avgRowSizeBytes);
            
            // 4. Calculate shard count using ShardPlanner
            ShardWorkerPlan plan = ShardPlanner.calculateOptimalShardWorkerPlan(
                    pipeline.getOptions(),
                    config.getMaximumPoolSize(),
                    stats.rowCount,
                    stats.avgRowSizeBytes,
                    null, // targetMbPerShard - use default
                    config.getShards(),
                    config.getWorkers(),
                    config.getMachineType()
            );
            
            int shardCount = plan.shardCount();
            log.info("Calculated shard plan: shards={}, workers={}, machineType={}, strategy={}", 
                    shardCount, plan.workerCount(), plan.machineType(), plan.calculationStrategy());
            
            // 5. Detect schema
            Schema baseSchema = detectSchema(dataSource, config);
            log.info("Detected schema with {} fields", baseSchema.getFieldCount());
            
            // 5a. Add shard_id field to schema for tracking
            Schema schema = Schema.builder()
                    .addFields(baseSchema.getFields())
                    .addField("shard_id", Schema.FieldType.INT32)
                    .build();
            log.info("Enhanced schema with shard_id field, total fields: {}", schema.getFieldCount());
            
            // 6. Create shards and read data
            PCollection<Integer> shards = pipeline
                    .apply("CreateShards", org.apache.beam.sdk.transforms.Create.of(
                            IntStream.range(0, shardCount).boxed().toList()));
            
            // 7. Read data using ParDo with sharding
            // Use static DoFn with serializable parameters to avoid serialization issues
            String query = buildShardedQuery(config);
            int fetchSize = config.getFetchSize() > 0 ? config.getFetchSize() : 5000;
            PCollection<Row> rows = shards.apply("ReadFromPostgres", 
                    ParDo.of(new ReadShardDoFn(dbConfig, query, shardCount, fetchSize, schema, baseSchema)))
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

    private TableStatistics estimateStatistics(DataSource dataSource, PipelineConfigSource config) {
        long statsStartTime = System.currentTimeMillis();
        
        try (Connection conn = dataSource.getConnection()) {
            String tableName = config.getTable();
            if (tableName == null || tableName.isBlank()) {
                // If no table specified, return default statistics
                return new TableStatistics(1000L, 200);
            }
            
            // Parse schema and table name
            String[] parts = tableName.split("\\.");
            String schemaName = parts.length > 1 ? parts[0] : "public";
            String tableNameOnly = parts.length > 1 ? parts[1] : parts[0];
            
            // Estimate row count from pg_class
            long rowCount = estimateRowCount(conn, schemaName, tableNameOnly);
            
            // Estimate average row size
            int avgRowSize = estimateAverageRowSize(conn, schemaName, tableNameOnly);
            
            long duration = System.currentTimeMillis() - statsStartTime;
            metricsCollector.recordStatisticsEstimation(duration);
            
            return new TableStatistics(rowCount, avgRowSize);
            
        } catch (SQLException e) {
            log.warn("Failed to estimate statistics, using defaults", e);
            return new TableStatistics(1000L, 200); // Default values
        }
    }

    private long estimateRowCount(Connection conn, String schemaName, String tableName) throws SQLException {
        String sql = "SELECT reltuples::bigint AS row_count " +
                    "FROM pg_class c " +
                    "JOIN pg_namespace n ON n.oid = c.relnamespace " +
                    "WHERE n.nspname = ? AND c.relname = ?";
        
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, schemaName);
            ps.setString(2, tableName);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getLong("row_count");
                }
            }
        }
        return 1000L; // Default if estimation fails
    }

    private int estimateAverageRowSize(Connection conn, String schemaName, String tableName) throws SQLException {
        String sql = "SELECT pg_size_pretty(pg_total_relation_size(c.oid)) AS total_size, " +
                    "reltuples::bigint AS row_count " +
                    "FROM pg_class c " +
                    "JOIN pg_namespace n ON n.oid = c.relnamespace " +
                    "WHERE n.nspname = ? AND c.relname = ?";
        
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, schemaName);
            ps.setString(2, tableName);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    long rowCount = rs.getLong("row_count");
                    if (rowCount > 0) {
                        // Get table size in bytes (simplified - actual implementation would parse pg_size_pretty)
                        // For now, use a reasonable default
                        return 200; // Default average row size
                    }
                }
            }
        }
        return 200; // Default
    }

    private Schema detectSchema(DataSource dataSource, PipelineConfigSource config) {
        long schemaStartTime = System.currentTimeMillis();
        
        try (Connection conn = dataSource.getConnection()) {
            String tableName = config.getTable();
            if (tableName == null || tableName.isBlank()) {
                throw new IllegalArgumentException("Table name is required");
            }
            
            // Parse schema and table name
            String[] parts = tableName.split("\\.");
            String schemaName = parts.length > 1 ? parts[0] : "public";
            String tableNameOnly = parts.length > 1 ? parts[1] : parts[0];
            
            List<Schema.Field> fields = new ArrayList<>();
            
            // Query to get column information
            String sql = "SELECT column_name, data_type, character_maximum_length " +
                        "FROM information_schema.columns " +
                        "WHERE table_schema = ? AND table_name = ? " +
                        "ORDER BY ordinal_position";
            
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, schemaName);
                ps.setString(2, tableNameOnly);
                try (ResultSet rs = ps.executeQuery()) {
                    int maxColumns = config.getMaxColumns() > 0 ? config.getMaxColumns() : Integer.MAX_VALUE;
                    int columnCount = 0;
                    
                    while (rs.next() && columnCount < maxColumns) {
                        String columnName = rs.getString("column_name");
                        String dataType = rs.getString("data_type");
                        // Use TypeConverter utility for type name mapping
                        Schema.FieldType fieldType = TypeConverter.mapPostgresTypeToBeamType(dataType);
                        fields.add(Schema.Field.of(columnName, fieldType).withNullable(true));
                        columnCount++;
                    }
                }
            }
            
            if (fields.isEmpty()) {
                throw new IllegalArgumentException("No columns found for table: " + tableName);
            }
            
            Schema schema = Schema.builder().addFields(fields).build();
            
            long duration = System.currentTimeMillis() - schemaStartTime;
            metricsCollector.recordSchemaDetection(duration);
            
            return schema;
            
        } catch (SQLException e) {
            log.error("Failed to detect schema", e);
            throw new RuntimeException("Failed to detect schema: " + e.getMessage(), e);
        }
    }

    private String buildShardedQuery(PipelineConfigSource config) {
        String tableName = config.getTable();
        if (tableName == null || tableName.isBlank()) {
            throw new IllegalArgumentException("Table name is required");
        }
        
        // Build hash-based sharded query
        // Uses PostgreSQL-specific hash function for sharding
        String shardColumn = config.getUpperBoundColumn();
        if (shardColumn == null || shardColumn.isBlank()) {
            // If no shard column specified, use a simple approach: use row number or primary key
            // For now, we'll use a simpler query that works without a specific column
            // This is a basic implementation - in production, you'd want to detect a good shard column
            return "SELECT * FROM " + tableName + " WHERE (abs(hashtext(ctid::text)) % ?) = ?";
        }
        
        // PostgreSQL hash-based sharding query with specified column
        return "SELECT * FROM " + tableName + " " +
               "WHERE ((((('x'||substr(md5(" + shardColumn + "::text),1,8))::bit(32))::bigint & 2147483647) % ?) = ?)";
    }

    private record TableStatistics(long rowCount, int avgRowSizeBytes) {}
    
    /**
     * Serializable DoFn for reading data from PostgreSQL shards.
     * This is a static class to avoid serialization issues with outer class references.
     */
    private static class ReadShardDoFn extends DoFn<Integer, Row> {
        private final DbConfigSnapshot dbConfig;
        private final String query;
        private final int shardCount;
        private final int fetchSize;
        private final Schema schema; // Full schema with shard_id
        private final Schema baseSchema; // Original schema without shard_id
        
        // Transient fields that will be initialized in @Setup
        private transient DataSource dataSource;
        private transient org.slf4j.Logger logger;
        
        ReadShardDoFn(DbConfigSnapshot dbConfig, String query, int shardCount, int fetchSize, 
                     Schema schema, Schema baseSchema) {
            this.dbConfig = dbConfig;
            this.query = query;
            this.shardCount = shardCount;
            this.fetchSize = fetchSize;
            this.schema = schema;
            this.baseSchema = baseSchema;
        }
        
        @Setup
        public void setup() {
            // Initialize DataSource in @Setup to avoid serialization issues
            this.dataSource = HikariDataSource.INSTANCE.getOrInit(dbConfig);
            this.logger = org.slf4j.LoggerFactory.getLogger(ReadShardDoFn.class);
        }
        
        @ProcessElement
        public void processElement(@Element Integer shardId, OutputReceiver<Row> out) {
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement ps = conn.prepareStatement(query)) {
                ps.setInt(1, shardCount); // Total shard count for modulo
                ps.setInt(2, shardId);    // This shard ID
                ps.setFetchSize(fetchSize);
                
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        Row row = mapResultSetToRow(rs, shardId);
                        out.output(row);
                    }
                }
            } catch (SQLException e) {
                throw new RuntimeException("Failed to read shard " + shardId, e);
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
