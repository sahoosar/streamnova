package com.di.streamnova.agent.metrics;

import com.di.streamnova.sql.SqlQueriesProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * JDBC implementation of MetricsLearningStore. Persists execution status (agent_runs),
 * estimates vs actuals (agent_estimates_vs_actuals), and throughput (agent_throughput_discovery)
 * to the same DB used by Flyway. Enable with streamnova.metrics.persistence-enabled=true and a configured datasource.
 */
@Component
@ConditionalOnProperty(name = "streamnova.metrics.persistence-enabled", havingValue = "true")
public class JdbcMetricsLearningStore implements MetricsLearningStore {

    private final JdbcTemplate jdbc;
    private final SqlQueriesProperties sql;

    public JdbcMetricsLearningStore(JdbcTemplate jdbcTemplate, SqlQueriesProperties sql) {
        this.jdbc = jdbcTemplate;
        this.sql = sql;
    }

    private static final RowMapper<ExecutionStatus> EXECUTION_STATUS_ROW_MAPPER = (rs, rowNum) -> ExecutionStatus.builder()
            .runId(rs.getString("run_id"))
            .profileRunId(rs.getString("profile_run_id"))
            .mode(rs.getString("mode"))
            .loadPattern(rs.getString("load_pattern"))
            .sourceType(rs.getString("source_type"))
            .schemaName(rs.getString("schema_name"))
            .tableName(rs.getString("table_name"))
            .status(rs.getString("status"))
            .startedAt(toInstant(rs.getTimestamp("started_at")))
            .finishedAt(toInstant(rs.getTimestamp("finished_at")))
            .createdAt(toInstant(rs.getTimestamp("created_at")))
            .jobId(rs.getString("job_id"))
            .message(rs.getString("message"))
            .callerAgentId(rs.getString("caller_agent_id"))
            .build();

    private static Instant toInstant(Timestamp ts) {
        return ts != null ? ts.toInstant() : null;
    }

    private static Timestamp toTimestamp(Instant i) {
        return i != null ? Timestamp.from(i) : null;
    }

    @Override
    public void saveExecutionStatus(ExecutionStatus status) {
        if (status == null || status.getRunId() == null) return;
        jdbc.update(
                sql.getMetrics().getInsertExecutionStatus(),
                status.getRunId(),
                status.getProfileRunId(),
                status.getMode(),
                status.getLoadPattern(),
                status.getSourceType(),
                status.getSchemaName(),
                status.getTableName(),
                status.getStatus(),
                toTimestamp(status.getStartedAt()),
                toTimestamp(status.getFinishedAt()),
                toTimestamp(status.getCreatedAt() != null ? status.getCreatedAt() : Instant.now()),
                status.getJobId(),
                status.getMessage(),
                status.getCallerAgentId());
    }

    @Override
    public void updateExecutionStatus(String runId, String status, Instant finishedAt, String jobId, String message) {
        if (runId == null) return;
        jdbc.update(
                sql.getMetrics().getUpdateExecutionStatus(),
                status, toTimestamp(finishedAt), jobId, message, runId);
    }

    @Override
    public Optional<ExecutionStatus> findExecutionStatusByRunId(String runId) {
        if (runId == null) return Optional.empty();
        List<ExecutionStatus> list = jdbc.query(
                sql.getMetrics().getFindExecutionByRunId(),
                EXECUTION_STATUS_ROW_MAPPER, runId);
        return list.isEmpty() ? Optional.empty() : Optional.of(list.get(0));
    }

    @Override
    public List<ExecutionStatus> findRecentExecutionStatuses(int limit) {
        return jdbc.query(
                sql.getMetrics().getFindRecentExecutions(),
                EXECUTION_STATUS_ROW_MAPPER, Math.max(1, limit));
    }

    @Override
    public List<ExecutionStatus> findExecutionStatusesByStatus(String status, int limit) {
        if (status == null) return findRecentExecutionStatuses(limit);
        return jdbc.query(
                sql.getMetrics().getFindExecutionsByStatus(),
                EXECUTION_STATUS_ROW_MAPPER, status, Math.max(1, limit));
    }

    @Override
    public List<ExecutionStatus> findExecutionStatusesByCallerAgentId(String callerAgentId, int limit) {
        int lim = Math.max(1, limit);
        if (callerAgentId == null || callerAgentId.isBlank()) {
            return findRecentExecutionStatuses(lim);
        }
        return jdbc.query(
                sql.getMetrics().getFindExecutionsByCallerAgentId(),
                EXECUTION_STATUS_ROW_MAPPER, callerAgentId.trim(), lim);
    }

    // --- Estimates vs actuals (agent_estimates_vs_actuals) ---
    @Override
    public void saveEstimateVsActual(EstimateVsActual record) {
        if (record == null || record.getRunId() == null) return;
        jdbc.update(
                sql.getMetrics().getInsertEstimateVsActual(),
                record.getRunId(),
                record.getEstimatedDurationSec(),
                record.getActualDurationSec(),
                record.getEstimatedCostUsd(),
                record.getActualCostUsd(),
                record.getMachineType(),
                record.getWorkerCount(),
                record.getShardCount(),
                toTimestamp(record.getRecordedAt()));
    }

    @Override
    public List<EstimateVsActual> findEstimatesVsActualsByRunId(String runId) {
        if (runId == null) return List.of();
        return jdbc.query(
                sql.getMetrics().getFindEstimatesVsActualsByRunId(),
                (rs, rowNum) -> EstimateVsActual.builder()
                        .runId(rs.getString("run_id"))
                        .estimatedDurationSec(rs.getObject("estimated_duration_sec", Double.class))
                        .actualDurationSec(rs.getObject("actual_duration_sec", Double.class))
                        .estimatedCostUsd(rs.getObject("estimated_cost_usd", Double.class))
                        .actualCostUsd(rs.getObject("actual_cost_usd", Double.class))
                        .machineType(rs.getString("machine_type"))
                        .workerCount(rs.getObject("worker_count", Integer.class))
                        .shardCount(rs.getObject("shard_count", Integer.class))
                        .recordedAt(toInstant(rs.getTimestamp("recorded_at")))
                        .build(),
                runId);
    }

    @Override
    public List<EstimateVsActual> findRecentEstimatesVsActuals(String sourceType, String schemaName, String tableName, int limit) {
        // agent_estimates_vs_actuals does not have source_type/schema/table; join with agent_runs for table scope
        var m = sql.getMetrics();
        String query = m.getEstimatesVsActualsBase();
        if (sourceType != null && !sourceType.isBlank()) query += m.getEstimatesVsActualsFilterSource();
        if (schemaName != null && !schemaName.isBlank()) query += m.getEstimatesVsActualsFilterSchema();
        if (tableName != null && !tableName.isBlank()) query += m.getEstimatesVsActualsFilterTable();
        query += m.getEstimatesVsActualsOrderLimit();
        List<Object> args = new java.util.ArrayList<>();
        if (sourceType != null && !sourceType.isBlank()) args.add(sourceType);
        if (schemaName != null && !schemaName.isBlank()) args.add(schemaName);
        if (tableName != null && !tableName.isBlank()) args.add(tableName);
        args.add(Math.max(1, limit));
        return jdbc.query(query, (rs, rowNum) -> EstimateVsActual.builder()
                .runId(rs.getString("run_id"))
                .estimatedDurationSec(rs.getObject("estimated_duration_sec", Double.class))
                .actualDurationSec(rs.getObject("actual_duration_sec", Double.class))
                .estimatedCostUsd(rs.getObject("estimated_cost_usd", Double.class))
                .actualCostUsd(rs.getObject("actual_cost_usd", Double.class))
                .machineType(rs.getString("machine_type"))
                .workerCount(rs.getObject("worker_count", Integer.class))
                .shardCount(rs.getObject("shard_count", Integer.class))
                .recordedAt(toInstant(rs.getTimestamp("recorded_at")))
                .build(), args.toArray());
    }

    // --- Throughput (agent_throughput_discovery) ---
    @Override
    public void saveThroughputProfile(ThroughputProfile profile) {
        if (profile == null || profile.getRunId() == null) return;
        jdbc.update(
                sql.getMetrics().getInsertThroughputProfile(),
                profile.getRunId(), profile.getSourceType(), profile.getSchemaName(), profile.getTableName(),
                profile.getBytesRead(), profile.getDurationMs(), profile.getRowsRead(), profile.getThroughputMbPerSec(),
                toTimestamp(profile.getSampledAt()), toTimestamp(Instant.now()));
    }

    @Override
    public Optional<ThroughputProfile> findThroughputProfileByRunId(String runId) {
        if (runId == null) return Optional.empty();
        List<ThroughputProfile> list = jdbc.query(
                sql.getMetrics().getFindThroughputByRunId(),
                (rs, rowNum) -> ThroughputProfile.builder()
                        .runId(rs.getString("run_id"))
                        .sourceType(rs.getString("source_type"))
                        .schemaName(rs.getString("schema_name"))
                        .tableName(rs.getString("table_name"))
                        .bytesRead(rs.getLong("bytes_read"))
                        .durationMs(rs.getLong("duration_ms"))
                        .rowsRead(rs.getInt("rows_read"))
                        .throughputMbPerSec(rs.getDouble("throughput_mb_per_sec"))
                        .sampledAt(toInstant(rs.getTimestamp("sampled_at")))
                        .build(),
                runId);
        return list.isEmpty() ? Optional.empty() : Optional.of(list.get(0));
    }

    @Override
    public List<ThroughputProfile> findRecentThroughputProfiles(String sourceType, String schemaName, String tableName, int limit) {
        var m = sql.getMetrics();
        String query = m.getThroughputBase();
        if (sourceType != null && !sourceType.isBlank()) query += m.getThroughputFilterSource();
        if (schemaName != null && !schemaName.isBlank()) query += m.getThroughputFilterSchema();
        if (tableName != null && !tableName.isBlank()) query += m.getThroughputFilterTable();
        query += m.getThroughputOrderLimit();
        List<Object> args = new java.util.ArrayList<>();
        if (sourceType != null && !sourceType.isBlank()) args.add(sourceType);
        if (schemaName != null && !schemaName.isBlank()) args.add(schemaName);
        if (tableName != null && !tableName.isBlank()) args.add(tableName);
        args.add(Math.max(1, limit));
        return jdbc.query(query, (rs, rowNum) -> ThroughputProfile.builder()
                .runId(rs.getString("run_id"))
                .sourceType(rs.getString("source_type"))
                .schemaName(rs.getString("schema_name"))
                .tableName(rs.getString("table_name"))
                .bytesRead(rs.getLong("bytes_read"))
                .durationMs(rs.getLong("duration_ms"))
                .rowsRead(rs.getInt("rows_read"))
                .throughputMbPerSec(rs.getDouble("throughput_mb_per_sec"))
                .sampledAt(toInstant(rs.getTimestamp("sampled_at")))
                .build(), args.toArray());
    }
}
