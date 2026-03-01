package com.di.streamnova.load.metadata;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.util.List;

/**
 * JDBC repository for the {@code load_shards} table.
 */
@Repository
@Slf4j
@RequiredArgsConstructor
public class LoadShardRepository {

    private final JdbcTemplate jdbc;

    // ------------------------------------------------------------------

    private static final RowMapper<LoadShard> ROW_MAPPER = (rs, n) -> {
        LoadShard s = new LoadShard();
        s.setId(rs.getString("id"));
        s.setRunId(rs.getString("run_id"));
        s.setShardIndex(rs.getInt("shard_index"));
        s.setIterationBorn(rs.getInt("iteration_born"));
        s.setPartitionCol(rs.getString("partition_col"));
        long minK = rs.getLong("min_key");
        s.setMinKey(rs.wasNull() ? null : minK);
        long maxK = rs.getLong("max_key");
        s.setMaxKey(rs.wasNull() ? null : maxK);
        long est  = rs.getLong("estimated_rows");
        s.setEstimatedRows(rs.wasNull() ? null : est);
        long act  = rs.getLong("actual_row_count");
        s.setActualRowCount(rs.wasNull() ? null : act);
        s.setGcsPath(rs.getString("gcs_path"));
        long fsz  = rs.getLong("gcs_file_size_bytes");
        s.setGcsFileSizeBytes(rs.wasNull() ? null : fsz);
        s.setStatus(rs.getString("status"));
        s.setAttempt(rs.getInt("attempt"));
        s.setErrorMessage(rs.getString("error_message"));
        Timestamp st  = rs.getTimestamp("started_at");
        s.setStartedAt(st  == null ? null : st.toInstant());
        Timestamp end = rs.getTimestamp("completed_at");
        s.setCompletedAt(end == null ? null : end.toInstant());
        long dur = rs.getLong("duration_ms");
        s.setDurationMs(rs.wasNull() ? null : dur);
        return s;
    };

    // ------------------------------------------------------------------
    // Bulk insert
    // ------------------------------------------------------------------

    /**
     * Inserts all shard rows in a single batch for the given run.
     */
    public void insertAll(List<LoadShard> shards) {
        jdbc.batchUpdate("""
            INSERT INTO load_shards
              (id, run_id, shard_index, iteration_born,
               partition_col, min_key, max_key, estimated_rows,
               status, attempt)
            VALUES (?,?,?,?, ?,?,?,?, ?,?)
            """,
            shards,
            shards.size(),
            (ps, s) -> {
                ps.setString(1, s.getId());
                ps.setString(2, s.getRunId());
                ps.setInt(3, s.getShardIndex());
                ps.setInt(4, s.getIterationBorn());
                ps.setString(5, s.getPartitionCol());
                if (s.getMinKey()        != null) ps.setLong(6,  s.getMinKey());        else ps.setNull(6,  java.sql.Types.BIGINT);
                if (s.getMaxKey()        != null) ps.setLong(7,  s.getMaxKey());        else ps.setNull(7,  java.sql.Types.BIGINT);
                if (s.getEstimatedRows() != null) ps.setLong(8,  s.getEstimatedRows()); else ps.setNull(8,  java.sql.Types.BIGINT);
                ps.setString(9,  s.getStatus());
                ps.setInt(10, s.getAttempt());
            });
    }

    // ------------------------------------------------------------------
    // Status updates (called from Stage 1 threads)
    // ------------------------------------------------------------------

    public void markRunning(String runId, int shardIndex, String gcsPath) {
        jdbc.update("""
            UPDATE load_shards
               SET status = 'RUNNING', gcs_path = ?, started_at = NOW()
             WHERE run_id = ? AND shard_index = ?
            """, gcsPath, runId, shardIndex);
    }

    public void markDone(String runId, int shardIndex,
                         long actualRowCount, long fileSizeBytes, long durationMs) {
        jdbc.update("""
            UPDATE load_shards
               SET status = 'DONE',
                   actual_row_count    = ?,
                   gcs_file_size_bytes = ?,
                   duration_ms         = ?,
                   completed_at        = NOW()
             WHERE run_id = ? AND shard_index = ?
            """, actualRowCount, fileSizeBytes, durationMs, runId, shardIndex);
    }

    public void markFailed(String runId, int shardIndex,
                           String errorMessage, long durationMs) {
        jdbc.update("""
            UPDATE load_shards
               SET status = 'FAILED', error_message = ?, duration_ms = ?, completed_at = NOW()
             WHERE run_id = ? AND shard_index = ?
            """, errorMessage, durationMs, runId, shardIndex);
    }

    public void markSkipped(String runId, int shardIndex) {
        jdbc.update("""
            UPDATE load_shards SET status = 'SKIPPED'
             WHERE run_id = ? AND shard_index = ?
            """, runId, shardIndex);
    }

    // ------------------------------------------------------------------
    // Read operations
    // ------------------------------------------------------------------

    public List<LoadShard> findByRunId(String runId) {
        return jdbc.query(
                "SELECT * FROM load_shards WHERE run_id = ? ORDER BY shard_index",
                ROW_MAPPER, runId);
    }

    public List<LoadShard> findDoneByRunId(String runId) {
        return jdbc.query(
                "SELECT * FROM load_shards WHERE run_id = ? AND status = 'DONE' ORDER BY shard_index",
                ROW_MAPPER, runId);
    }

    public long countByStatus(String runId, String status) {
        Long cnt = jdbc.queryForObject(
                "SELECT COUNT(*) FROM load_shards WHERE run_id = ? AND status = ?",
                Long.class, runId, status);
        return cnt == null ? 0L : cnt;
    }

    public long sumActualRows(String runId) {
        Long sum = jdbc.queryForObject(
                "SELECT COALESCE(SUM(actual_row_count),0) FROM load_shards"
              + " WHERE run_id = ? AND status = 'DONE'",
                Long.class, runId);
        return sum == null ? 0L : sum;
    }

    public long sumFileSizeBytes(String runId) {
        Long sum = jdbc.queryForObject(
                "SELECT COALESCE(SUM(gcs_file_size_bytes),0) FROM load_shards"
              + " WHERE run_id = ? AND status = 'DONE'",
                Long.class, runId);
        return sum == null ? 0L : sum;
    }
}
