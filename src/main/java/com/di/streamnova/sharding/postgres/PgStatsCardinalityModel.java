package com.di.streamnova.sharding.postgres;

import com.di.streamnova.sharding.api.CardinalityModel;
import com.di.streamnova.sharding.api.ColumnSelection;
import java.sql.*;
import java.util.List;
import java.util.stream.Collectors;


public class PgStatsCardinalityModel implements CardinalityModel {
    {
   /* @Override
    public ColumnSelection pickShardColumns(Connection con, String schema, String table,
                                            List<String> allowlist, int k,
                                            long minDistinct, double maxNullFrac,
                                            boolean preferPk) throws SQLException {

        Objects.requireNonNull(con, "Connection required");
        schema = norm(schema);
        table  = norm(table);
        k      = Math.max(1, k);

        // Make metadata queries safe & bounded
        boolean origAuto = con.getAutoCommit();
        con.setAutoCommit(false);
        try (Statement s = con.createStatement()) {
            // Works on PostgreSQL; SET LOCAL applies only inside this txn
            s.execute("SET LOCAL transaction_read_only = on");
        } catch (SQLException ignore) {
            // Older versions/permissions: best effort
        }

        List<String> pkCols = findPk(con, schema, table);
        List<ColumnStat> stats = fetchStats(con, schema, table, 3 *//*seconds timeout*//*);

        // Apply allowlist & thresholds
        List<ColumnStat> candidates = stats.stream()
                .filter(c -> allowlist == null || allowlist.isEmpty() || allowlist.contains(c.name()))
                .filter(c -> c.estDistinct() >= minDistinct)
                .filter(c -> c.nullFrac() <= maxNullFrac)
                .toList();

        // Prefer PK if requested and present
        if (preferPk && !pkCols.isEmpty()) {
            List<String> chosen = pkCols.size() > k ? pkCols.subList(0, k) : pkCols;
            restoreAutoCommit(con, origAuto);
            return new ColumnSelection(chosen, stats, "Picked primary key columns");
        }

        // Rank by estimated distinct desc, null frac asc, name asc
        List<String> chosen = candidates.stream()
                .sorted((a, b) -> {
                    int cmp = Double.compare(b.estDistinct(), a.estDistinct());
                    if (cmp != 0) return cmp;
                    cmp = Double.compare(a.nullFrac(), b.nullFrac());
                    return (cmp != 0) ? cmp : a.name().compareTo(b.name());
                })
                .limit(k)
                .map(ColumnStat::name)
                .collect(Collectors.toList());

        restoreAutoCommit(con, origAuto);
        if (chosen.isEmpty()) {
            return new ColumnSelection(chosen, stats, "No columns met thresholds");
        }
        return new ColumnSelection(chosen, stats, "Picked by pg_stats ranking");
    }

    *//** Pull PK column names in ordinal order. *//*
    private static List<String> findPk(Connection con, String schema, String table) throws SQLException {
        final String sql = """
            SELECT a.attname
            FROM pg_index i
            JOIN pg_class c ON c.oid = i.indrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE n.nspname = ? AND c.relname = ? AND i.indisprimary
            ORDER BY a.attnum
            """;
        try (PreparedStatement ps = con.prepareStatement(sql)) {
            ps.setString(1, schema);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                List<String> out = new ArrayList<>();
                while (rs.next()) out.add(rs.getString(1));
                return out;
            }
        }
    }

    *//** Read per-column estimated distincts/nulls for string/number columns from pg_stats. *//*
    private static List<ColumnStat> fetchStats(Connection con, String schema, String table, int timeoutSec) throws SQLException {
        final String sql = """
            WITH base AS (
              SELECT a.attname AS col,
                     (SELECT typcategory FROM pg_type WHERE oid = a.atttypid) AS typcat,
                     s.n_distinct,
                     COALESCE(s.null_frac, 0.0) AS null_frac,
                     c.reltuples::numeric AS reltuples,
                     EXISTS (
                       SELECT 1 FROM pg_index i
                       WHERE i.indrelid = a.attrelid AND i.indisprimary
                         AND a.attnum = ANY(i.indkey)
                     ) AS in_pk
              FROM pg_attribute a
              JOIN pg_class     c ON c.oid = a.attrelid
              JOIN pg_namespace n ON n.oid = c.relnamespace
              LEFT JOIN pg_stats s
                ON s.schemaname = n.nspname AND s.tablename = c.relname AND s.attname = a.attname
              WHERE n.nspname = ? AND c.relname = ? AND a.attnum > 0 AND NOT a.attisdropped
            )
            SELECT col,
                   CASE
                     WHEN n_distinct IS NULL THEN 0
                     WHEN n_distinct >= 0     THEN n_distinct
                     ELSE (-n_distinct) * reltuples
                   END AS est_distinct,
                   null_frac,
                   in_pk
            FROM base
            WHERE typcat IN ('S','N')   -- strings & numbers only
            """;
        try (PreparedStatement ps = con.prepareStatement(sql)) {
            ps.setQueryTimeout(Math.max(1, timeoutSec)); // seconds
            ps.setString(1, schema);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                List<ColumnStat> out = new ArrayList<>();
                while (rs.next()) {
                    out.add(new ColumnStat(
                            rs.getString("col"),
                            rs.getDouble("est_distinct"),
                            rs.getDouble("null_frac"),
                            rs.getBoolean("in_pk")));
                }
                return out;
            }
        }
    }

    private static void restoreAutoCommit(Connection con, boolean original) {
        try { con.setAutoCommit(original); } catch (SQLException ignore) {}
    }

    private static String norm(String ident) {
        if (ident == null) return null;
        String trimmed = ident.trim();
        // Unquoted identifiers are folded to lowercase by PG; keep case for quoted ones
        if (trimmed.startsWith("\"") && trimmed.endsWith("\"") && trimmed.length() >= 2) {
            return trimmed.substring(1, trimmed.length() - 1).replace("\"\"", "\"");
        }
        return trimmed.toLowerCase(Locale.ROOT);
    }
}*/
    }

    @Override
    public ColumnSelection pickShardColumns(Connection con, String schema, String table, List<String> allowlist, int k, long minDistinct, double maxNullFrac, boolean preferPk) throws SQLException {
        return null;
    }

}