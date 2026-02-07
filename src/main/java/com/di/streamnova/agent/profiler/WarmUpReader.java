package com.di.streamnova.agent.profiler;

import com.di.streamnova.config.PipelineConfigSource;
import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.regex.Pattern;

/**
 * Performs a small read (warm-up) against Postgres to discover read throughput.
 * Used by the Profiler to feed the Estimator and later the Metrics & Learning Store.
 */
@Slf4j
public final class WarmUpReader {

    private static final String LOG_PREFIX = "[PROFILER-WARMUP]";
    private static final Pattern SAFE_IDENTIFIER = Pattern.compile("^[a-zA-Z0-9_]+$");

    /** Default number of rows to read in warm-up (enough to get stable throughput). */
    public static final int DEFAULT_WARM_UP_ROWS = 5_000;
    /** Default fetch size for the warm-up query. */
    public static final int DEFAULT_FETCH_SIZE = 1_000;

    private WarmUpReader() {}

    /**
     * Runs a warm-up read using a one-off connection (no pool).
     *
     * @param config         pipeline source config (table, jdbcUrl, username, password, driver)
     * @param avgRowSizeBytes average row size from table profile (used to estimate bytes read)
     * @return throughput sample, or null if table is empty or read fails
     */
    public static ThroughputSample runWarmUp(PipelineConfigSource config, int avgRowSizeBytes) {
        return runWarmUp(config, avgRowSizeBytes, DEFAULT_WARM_UP_ROWS, DEFAULT_FETCH_SIZE);
    }

    /**
     * Runs a warm-up read with explicit row and fetch size.
     *
     * @param config         pipeline source config
     * @param avgRowSizeBytes average row size for byte estimation
     * @param warmUpRows      max rows to read
     * @param fetchSize       JDBC fetch size
     * @return throughput sample, or null if empty or error
     */
    public static ThroughputSample runWarmUp(PipelineConfigSource config, int avgRowSizeBytes,
                                             int warmUpRows, int fetchSize) {
        if (config == null || config.getTable() == null || config.getTable().isBlank()) {
            log.warn("{} Skipping warm-up: no table config", LOG_PREFIX);
            return null;
        }
        TableRef ref = TableRef.from(config.getTable());
        String driver = config.getDriver() != null && !config.getDriver().isBlank() ? config.getDriver() : "org.postgresql.Driver";
        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            log.warn("{} Driver not found: {}", LOG_PREFIX, driver, e);
            return null;
        }
        try (Connection conn = DriverManager.getConnection(
                config.getJdbcUrl(), config.getUsername(), config.getPassword())) {
            return runWarmUpFromConnection(conn, ref, "postgres", avgRowSizeBytes, warmUpRows, fetchSize);
        } catch (SQLException e) {
            log.warn("{} Warm-up connection failed: {}", LOG_PREFIX, e.getMessage());
            return null;
        }
    }

    /**
     * Runs a warm-up read using a connection from the given DataSource.
     */
    public static ThroughputSample runWarmUp(DataSource dataSource, PipelineConfigSource config, int avgRowSizeBytes) {
        if (dataSource == null || config == null || config.getTable() == null || config.getTable().isBlank()) {
            log.warn("{} Skipping warm-up: no datasource or table", LOG_PREFIX);
            return null;
        }
        TableRef ref = TableRef.from(config.getTable());
        try (Connection conn = dataSource.getConnection()) {
            return runWarmUpFromConnection(conn, ref, "postgres", avgRowSizeBytes, DEFAULT_WARM_UP_ROWS, DEFAULT_FETCH_SIZE);
        } catch (SQLException e) {
            log.warn("{} Warm-up connection failed: {}", LOG_PREFIX, e.getMessage());
            return null;
        }
    }

    private static ThroughputSample runWarmUpFromConnection(Connection conn, TableRef ref, String sourceType,
                                                             int avgRowSizeBytes, int warmUpRows, int fetchSize) throws SQLException {
        String sql = "SELECT * FROM \"" + ref.schema + "\".\"" + ref.table + "\" LIMIT " + Math.max(1, warmUpRows);
        long startMs = System.currentTimeMillis();
        int rowsRead = 0;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setFetchSize(Math.min(fetchSize, warmUpRows));
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    rowsRead++;
                }
            }
        }
        long durationMs = System.currentTimeMillis() - startMs;
        long estimatedBytes = (long) rowsRead * Math.max(1, avgRowSizeBytes);
        ThroughputSample sample = ThroughputSample.of(
                estimatedBytes, durationMs, rowsRead,
                sourceType, ref.schema, ref.table);
        log.info("{} {} rows in {} ms → ~{} MB/s (estimated {} bytes)",
                LOG_PREFIX, rowsRead, durationMs, String.format("%.2f", sample.getThroughputMbPerSec()), estimatedBytes);
        return sample;
    }

    private static final class TableRef {
        final String schema;
        final String table;

        TableRef(String schema, String table) {
            this.schema = schema;
            this.table = table;
        }

        static TableRef from(String fullTableName) {
            if (fullTableName == null || fullTableName.isBlank()) {
                throw new IllegalArgumentException(LOG_PREFIX + " Table name is required");
            }
            String trimmed = fullTableName.trim();
            String[] parts = trimmed.split("\\.", -1);
            String schema = parts.length > 1 ? parts[0].trim() : "public";
            String table = parts.length > 1 ? parts[1].trim() : trimmed;
            if (table.isEmpty()) {
                throw new IllegalArgumentException(LOG_PREFIX + " Invalid table name: " + fullTableName);
            }
            if (!SAFE_IDENTIFIER.matcher(schema).matches() || !SAFE_IDENTIFIER.matcher(table).matches()) {
                throw new IllegalArgumentException(LOG_PREFIX + " Schema and table must be alphanumeric");
            }
            return new TableRef(schema, table);
        }
    }
}
