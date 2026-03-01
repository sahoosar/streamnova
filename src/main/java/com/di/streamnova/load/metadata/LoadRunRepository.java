package com.di.streamnova.load.metadata;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.util.List;
import java.util.Optional;

/**
 * JDBC repository for the {@code load_runs} table.
 */
@Repository
@Slf4j
@RequiredArgsConstructor
public class LoadRunRepository {

    private final JdbcTemplate jdbc;

    // ------------------------------------------------------------------
    // RowMapper
    // ------------------------------------------------------------------

    private static final RowMapper<LoadRun> ROW_MAPPER = (rs, n) -> {
        LoadRun r = new LoadRun();
        r.setId(rs.getString("id"));
        r.setTableSchema(rs.getString("table_schema"));
        r.setTableName(rs.getString("table_name"));
        r.setPartitionCol(rs.getString("partition_col"));
        r.setSourceRowCount(nullableLong(rs.getLong("source_row_count"), rs.wasNull()));
        r.setAvgRowBytes(nullableInt(rs.getInt("avg_row_bytes"), rs.wasNull()));
        r.setTotalDataBytes(nullableLong(rs.getLong("total_data_bytes"), rs.wasNull()));
        r.setMachineType(rs.getString("machine_type"));
        r.setWorkerCount(nullableInt(rs.getInt("worker_count"), rs.wasNull()));
        r.setShardCount(nullableInt(rs.getInt("shard_count"), rs.wasNull()));
        r.setMaxConcurrent(nullableInt(rs.getInt("max_concurrent"), rs.wasNull()));
        r.setPoolSize(rs.getInt("pool_size"));
        r.setIterationCount(rs.getInt("iteration_count"));
        r.setStatus(rs.getString("status"));
        r.setErrorMessage(rs.getString("error_message"));
        r.setGcsBucket(rs.getString("gcs_bucket"));
        r.setGcsPrefix(rs.getString("gcs_prefix"));
        r.setBqProject(rs.getString("bq_project"));
        r.setBqDataset(rs.getString("bq_dataset"));
        r.setBqTable(rs.getString("bq_table"));
        r.setWriteDisposition(rs.getString("write_disposition"));
        r.setStartedAt(toInstant(rs.getTimestamp("started_at")));
        r.setStage1StartedAt(toInstant(rs.getTimestamp("stage1_started_at")));
        r.setStage1CompletedAt(toInstant(rs.getTimestamp("stage1_completed_at")));
        r.setStage2CompletedAt(toInstant(rs.getTimestamp("stage2_completed_at")));
        r.setStage3StartedAt(toInstant(rs.getTimestamp("stage3_started_at")));
        r.setStage3CompletedAt(toInstant(rs.getTimestamp("stage3_completed_at")));
        r.setPromotedAt(toInstant(rs.getTimestamp("promoted_at")));
        r.setPlanIterationsJson(rs.getString("plan_iterations_json"));
        return r;
    };

    // ------------------------------------------------------------------
    // Write operations
    // ------------------------------------------------------------------

    public void insert(LoadRun run) {
        jdbc.update("""
            INSERT INTO load_runs
              (id, table_schema, table_name, partition_col,
               source_row_count, avg_row_bytes, total_data_bytes,
               machine_type, worker_count, shard_count, max_concurrent, pool_size,
               iteration_count, status,
               gcs_bucket, gcs_prefix, bq_project, bq_dataset, bq_table, write_disposition,
               plan_iterations_json)
            VALUES (?,?,?,?, ?,?,?, ?,?,?,?,?, ?,?, ?,?,?,?,?,?, ?)
            """,
            run.getId(), run.getTableSchema(), run.getTableName(), run.getPartitionCol(),
            run.getSourceRowCount(), run.getAvgRowBytes(), run.getTotalDataBytes(),
            run.getMachineType(), run.getWorkerCount(), run.getShardCount(),
            run.getMaxConcurrent(), run.getPoolSize(),
            run.getIterationCount(), run.getStatus(),
            run.getGcsBucket(), run.getGcsPrefix(),
            run.getBqProject(), run.getBqDataset(), run.getBqTable(), run.getWriteDisposition(),
            run.getPlanIterationsJson());
    }

    /**
     * Persists the profiler results (row count, avg row size, total data bytes)
     * after the table has been sampled.  Called once per run, right after
     * {@link #insert}, so these fields are no longer null in the audit record.
     */
    public void updateProfileStats(String id,
                                   long totalRows,
                                   long avgRowBytes,
                                   long totalDataBytes) {
        jdbc.update("""
            UPDATE load_runs
               SET source_row_count = ?,
                   avg_row_bytes    = ?,
                   total_data_bytes = ?
             WHERE id = ?
            """, totalRows, avgRowBytes, totalDataBytes, id);
    }

    public void updatePlan(String id, int iterationCount, int shardCount,
                           int maxConcurrent, String planJson) {
        jdbc.update("""
            UPDATE load_runs
               SET iteration_count = ?, shard_count = ?, max_concurrent = ?,
                   plan_iterations_json = ?
             WHERE id = ?
            """, iterationCount, shardCount, maxConcurrent, planJson, id);
    }

    public void updateStatus(String id, String status) {
        jdbc.update("UPDATE load_runs SET status = ? WHERE id = ?", status, id);
    }

    public void markStage1Running(String id) {
        jdbc.update("""
            UPDATE load_runs SET status = 'STAGE1_RUNNING', stage1_started_at = NOW()
             WHERE id = ?
            """, id);
    }

    public void markStage1Done(String id) {
        jdbc.update("""
            UPDATE load_runs SET status = 'STAGE1_DONE', stage1_completed_at = NOW()
             WHERE id = ?
            """, id);
    }

    public void markStage2Running(String id) {
        jdbc.update("UPDATE load_runs SET status = 'STAGE2_RUNNING' WHERE id = ?", id);
    }

    public void markStage2Done(String id) {
        jdbc.update("""
            UPDATE load_runs SET status = 'STAGE2_DONE', stage2_completed_at = NOW()
             WHERE id = ?
            """, id);
    }

    public void markStage3Running(String id) {
        jdbc.update("""
            UPDATE load_runs SET status = 'STAGE3_RUNNING', stage3_started_at = NOW()
             WHERE id = ?
            """, id);
    }

    public void markStage3Done(String id) {
        jdbc.update("""
            UPDATE load_runs SET status = 'STAGE3_DONE', stage3_completed_at = NOW()
             WHERE id = ?
            """, id);
    }

    public void markPromoting(String id) {
        jdbc.update("UPDATE load_runs SET status = 'PROMOTING' WHERE id = ?", id);
    }

    public void markPromoted(String id) {
        jdbc.update("""
            UPDATE load_runs SET status = 'PROMOTED', promoted_at = NOW()
             WHERE id = ?
            """, id);
    }

    public void markFailed(String id, String errorMessage) {
        jdbc.update("""
            UPDATE load_runs SET status = 'FAILED', error_message = ?
             WHERE id = ?
            """, errorMessage, id);
    }

    // ------------------------------------------------------------------
    // Read operations
    // ------------------------------------------------------------------

    public Optional<LoadRun> findById(String id) {
        List<LoadRun> rows = jdbc.query(
                "SELECT * FROM load_runs WHERE id = ?", ROW_MAPPER, id);
        return rows.isEmpty() ? Optional.empty() : Optional.of(rows.get(0));
    }

    public List<LoadRun> findByStatus(String status) {
        return jdbc.query(
                "SELECT * FROM load_runs WHERE status = ? ORDER BY started_at DESC",
                ROW_MAPPER, status);
    }

    public List<LoadRun> findByTable(String schema, String table) {
        return jdbc.query(
                "SELECT * FROM load_runs WHERE table_schema = ? AND table_name = ?"
              + " ORDER BY started_at DESC",
                ROW_MAPPER, schema, table);
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    private static java.time.Instant toInstant(Timestamp ts) {
        return ts == null ? null : ts.toInstant();
    }
    private static Long    nullableLong(long v, boolean wasNull) { return wasNull ? null : v; }
    private static Integer nullableInt(int  v, boolean wasNull)  { return wasNull ? null : v; }
}
