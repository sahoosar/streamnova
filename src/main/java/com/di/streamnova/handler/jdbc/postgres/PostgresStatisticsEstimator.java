package com.di.streamnova.handler.jdbc.postgres;

import com.di.streamnova.config.PipelineConfigSource;
import com.di.streamnova.util.ConnectionPoolLogger;
import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.regex.Pattern;

/**
 * Estimates table statistics for PostgreSQL before loading data.
 * Used for shard planning (row count and average row size).
 * <p>
 * Production: validates input, uses parameterized queries, structured logging, safe fallbacks.
 */
@Slf4j
public final class PostgresStatisticsEstimator {

    private static final String LOG_PREFIX = "[STATS]";
    private static final String DEFAULT_DRIVER = "org.postgresql.Driver";
    private static final int DEFAULT_MIN_AVG_ROW_BYTES = 200;
    private static final int SAMPLE_SIZE = 1000;
    private static final int PG_PAGE_BYTES = 8192;

    /** Allowed for schema/table names (safe for quoting in SQL). */
    private static final Pattern SAFE_IDENTIFIER = Pattern.compile("^[a-zA-Z0-9_]+$");

    private PostgresStatisticsEstimator() {}

    /**
     * Table statistics returned before loading data.
     */
    public record TableStatistics(long rowCount, int avgRowSizeBytes) {}

    /**
     * Parsed and validated schema + table identifier.
     */
    private record TableRef(String schema, String table) {
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
                throw new IllegalArgumentException(LOG_PREFIX + " Schema and table must be alphanumeric (got schema='" + schema + "', table='" + table + "')");
            }
            return new TableRef(schema, table);
        }
    }

    // --- Public API ---

    /**
     * Estimates statistics using a one-off connection (no pool).
     */
    public static TableStatistics estimateStatistics(PipelineConfigSource config) {
        ensureDriverLoaded(config.getDriver());
        try (Connection conn = DriverManager.getConnection(
                config.getJdbcUrl(), config.getUsername(), config.getPassword())) {
            return estimateFromConnection(conn, config);
        } catch (SQLException e) {
            log.error("{} Failed to connect for statistics | table={} | error={}",
                    LOG_PREFIX, config.getTable(), e.getMessage(), e);
            throw new StatisticsException("Statistics estimation failed: " + e.getMessage(), e);
        }
    }

    /**
     * Estimates statistics using a connection from the given DataSource.
     */
    public static TableStatistics estimateStatistics(DataSource dataSource, PipelineConfigSource config) {
        try (Connection conn = dataSource.getConnection()) {
            TableStatistics stats = estimateFromConnection(conn, config);
            ConnectionPoolLogger.logPoolStats(dataSource, "after statistics (1 conn used)");
            return stats;
        } catch (SQLException e) {
            log.error("{} Failed to get connection for statistics | table={} | error={}",
                    LOG_PREFIX, config.getTable(), e.getMessage(), e);
            throw new StatisticsException("Statistics estimation failed: " + e.getMessage(), e);
        }
    }

    // --- Core logic ---

    private static void ensureDriverLoaded(String driver) {
        String className = (driver != null && !driver.isBlank()) ? driver : DEFAULT_DRIVER;
        try {
            Class.forName(className);
        } catch (ClassNotFoundException e) {
            log.error("{} JDBC driver not found: {}", LOG_PREFIX, className, e);
            throw new StatisticsException("JDBC driver not found: " + className, e);
        }
    }

    private static TableStatistics estimateFromConnection(Connection conn, PipelineConfigSource config) throws SQLException {
        TableRef ref = TableRef.from(config.getTable());
        log.info("{} Estimating table '{}.{}'", LOG_PREFIX, ref.schema(), ref.table());

        long rowCount = estimateRowCount(conn, ref);
        int avgRowSize = estimateAverageRowSize(conn, ref);

        log.info("{} Result | table='{}.{}' | rowCount={}, avgRowSizeBytes={}",
                LOG_PREFIX, ref.schema(), ref.table(), rowCount, avgRowSize);
        return new TableStatistics(rowCount, avgRowSize);
    }

    private static long estimateRowCount(Connection conn, TableRef ref) throws SQLException {
        Long fromStats = rowCountFromPgClass(conn, ref);
        if (fromStats != null && fromStats > 0) {
            log.debug("{} Row count from pg_class: {}", LOG_PREFIX, fromStats);
            return fromStats;
        }
        long fromCount = rowCountFromCountStar(conn, ref);
        if (fromStats != null && fromStats == 0) {
            log.warn("{} pg_class returned 0; COUNT(*)={}", LOG_PREFIX, fromCount);
        } else {
            log.debug("{} Row count from COUNT(*): {}", LOG_PREFIX, fromCount);
        }
        return fromCount;
    }

    private static Long rowCountFromPgClass(Connection conn, TableRef ref) throws SQLException {
        String sql = "SELECT reltuples::bigint AS row_count FROM pg_class c " +
                "JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = ? AND c.relname = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, ref.schema());
            ps.setString(2, ref.table());
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getLong("row_count") : null;
            }
        }
    }

    private static long rowCountFromCountStar(Connection conn, TableRef ref) throws SQLException {
        String sql = "SELECT COUNT(*) AS row_count FROM \"" + ref.schema() + "\".\"" + ref.table() + "\"";
        try (PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            return rs.next() ? rs.getLong("row_count") : 0L;
        }
    }

    private static int estimateAverageRowSize(Connection conn, TableRef ref) throws SQLException {
        Integer fromStats = avgRowSizeFromPgClass(conn, ref);
        if (fromStats != null && fromStats > 0) {
            log.debug("{} Avg row size from pg_class: {} bytes", LOG_PREFIX, fromStats);
            return fromStats;
        }
        int fromSample = avgRowSizeFromSample(conn, ref);
        if (fromStats != null && (fromStats == null || fromStats == 0)) {
            log.warn("{} pg_class avg row size unavailable; using sample: {} bytes", LOG_PREFIX, fromSample);
        } else {
            log.debug("{} Avg row size from sample: {} bytes", LOG_PREFIX, fromSample);
        }
        return fromSample;
    }

    /**
     * Average row size from pg_class: (relpages * page_size) / reltuples.
     * Returns null if no row or value is zero.
     */
    private static Integer avgRowSizeFromPgClass(Connection conn, TableRef ref) throws SQLException {
        // relpages = number of disk pages; 8192 bytes per page → total table size / row count = avg row size
        String sql =
                "SELECT CASE WHEN c.reltuples > 0 " +
                "        THEN (c.relpages::bigint * ?) / c.reltuples " +
                "        ELSE 0 " +
                "       END AS avg_row_size " +
                "FROM pg_class c " +
                "JOIN pg_namespace n ON n.oid = c.relnamespace " +
                "WHERE n.nspname = ? AND c.relname = ?";

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, PG_PAGE_BYTES);
            ps.setString(2, ref.schema());
            ps.setString(3, ref.table());

            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                int avgRowSize = rs.getInt("avg_row_size");
                return avgRowSize > 0 ? avgRowSize : null;
            }
        }
    }

    private static int avgRowSizeFromSample(Connection conn, TableRef ref) throws SQLException {
        String sql = "SELECT pg_column_size(t.*) AS row_size FROM \"" + ref.schema() + "\".\"" + ref.table() + "\" t LIMIT " + SAMPLE_SIZE;
        try (PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            long sum = 0;
            int n = 0;
            while (rs.next()) {
                sum += rs.getInt("row_size");
                n++;
            }
            if (n == 0) {
                log.warn("{} No rows sampled; using default avg row size {} bytes", LOG_PREFIX, DEFAULT_MIN_AVG_ROW_BYTES);
                return DEFAULT_MIN_AVG_ROW_BYTES;
            }
            return Math.max((int) (sum / n), DEFAULT_MIN_AVG_ROW_BYTES);
        }
    }

    /**
     * Thrown when statistics estimation fails (connection, driver, or validation).
     */
    public static final class StatisticsException extends RuntimeException {
        public StatisticsException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
