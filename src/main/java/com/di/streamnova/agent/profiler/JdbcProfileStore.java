package com.di.streamnova.agent.profiler;

import com.di.streamnova.sql.SqlQueriesProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * JDBC implementation of {@link ProfileStore} using {@code profile_run_meta},
 * {@code agent_table_profiles}, and {@code agent_throughput_discovery}.
 * Active when {@code streamnova.metrics.persistence-enabled=true}.
 */
@Service
@ConditionalOnProperty(name = "streamnova.metrics.persistence-enabled", havingValue = "true")
public class JdbcProfileStore implements ProfileStore {

    private final JdbcTemplate jdbc;
    private final SqlQueriesProperties sql;

    public JdbcProfileStore(JdbcTemplate jdbcTemplate, SqlQueriesProperties sql) {
        this.jdbc = jdbcTemplate;
        this.sql = sql;
    }

    @Override
    public String save(ProfileResult result) {
        if (result == null || result.getRunId() == null) return null;
        String runId = result.getRunId();
        Instant completedAt = result.getCompletedAt() != null ? result.getCompletedAt() : Instant.now();
        String errorMessage = result.getErrorMessage().orElse(null);
        String sourceType = null;
        String schemaName = null;
        String tableName = null;
        if (result.getTableProfile() != null) {
            sourceType = result.getTableProfile().getSourceType();
            schemaName = result.getTableProfile().getSchemaName();
            tableName = result.getTableProfile().getTableName();
        }

        try {
            jdbc.update(
                    sql.getProfile().getInsertRunMeta(),
                    runId, Timestamp.from(completedAt), errorMessage, sourceType, schemaName, tableName);

            if (result.getTableProfile() != null) {
                TableProfile p = result.getTableProfile();
                jdbc.update(
                        sql.getProfile().getInsertTableProfile(),
                        runId, p.getSourceType(), p.getSchemaName(), p.getTableName(),
                        p.getRowCountEstimate(), p.getAvgRowSizeBytes(), p.getEstimatedTotalBytes(),
                        p.getComplexity(), Timestamp.from(p.getProfiledAt()));
            }

            if (result.getThroughputSample().isPresent()) {
                ThroughputSample s = result.getThroughputSample().get();
                jdbc.update(
                        sql.getProfile().getInsertThroughput(),
                        runId, s.getSourceType(), s.getSchemaName(), s.getTableName(),
                        s.getBytesRead(), s.getDurationMs(), s.getRowsRead(), s.getThroughputMbPerSec(),
                        Timestamp.from(s.getSampledAt()));
            }
            return runId;
        } catch (Exception e) {
            org.slf4j.LoggerFactory.getLogger(JdbcProfileStore.class)
                    .warn("[PROFILE_STORE] Failed to save profile runId={}: {}", runId, e.getMessage());
            return null;
        }
    }

    @Override
    public Optional<ProfileResult> findByRunId(String runId) {
        if (runId == null || runId.isBlank()) return Optional.empty();
        try {
            List<ProfileRunMeta> metaList = jdbc.query(
                    sql.getProfile().getFindRunMetaByRunId(),
                    ProfileRunMeta.ROW_MAPPER, runId);
            if (metaList.isEmpty()) return Optional.empty();
            ProfileRunMeta meta = metaList.get(0);

            TableProfile tableProfile = null;
            List<TableProfile> tableProfiles = jdbc.query(
                    sql.getProfile().getFindTableProfileByRunId(),
                    TableProfileRowMapper.INSTANCE, runId);
            if (!tableProfiles.isEmpty()) tableProfile = tableProfiles.get(0);

            ThroughputSample throughputSample = null;
            List<ThroughputSample> samples = jdbc.query(
                    sql.getProfile().getFindThroughputByRunId(),
                    ThroughputSampleRowMapper.INSTANCE, runId);
            if (!samples.isEmpty()) throughputSample = samples.get(0);

            ProfileResult result = ProfileResult.builder()
                    .runId(runId)
                    .tableProfile(tableProfile)
                    .throughputSample(throughputSample)
                    .completedAt(meta.completedAt)
                    .errorMessage(meta.errorMessage)
                    .build();
            return Optional.of(result);
        } catch (Exception e) {
            org.slf4j.LoggerFactory.getLogger(JdbcProfileStore.class)
                    .warn("[PROFILE_STORE] Failed to find runId={}: {}", runId, e.getMessage());
            return Optional.empty();
        }
    }

    @Override
    public List<ProfileResult> findRecentByTable(String sourceType, String schemaName, String tableName, int limit) {
        if (limit <= 0) return List.of();
        int safeLimit = Math.min(limit, 500);
        try {
            List<String> runIds = jdbc.query(
                    sql.getProfile().getFindRunIdsByTable(),
                    (rs, rowNum) -> rs.getString("run_id"),
                    sourceType, schemaName, tableName, safeLimit);

            List<ProfileResult> results = new ArrayList<>();
            for (String runId : runIds) {
                findByRunId(runId).ifPresent(results::add);
            }
            return results;
        } catch (Exception e) {
            org.slf4j.LoggerFactory.getLogger(JdbcProfileStore.class)
                    .warn("[PROFILE_STORE] Failed to findRecentByTable: {}", e.getMessage());
            return List.of();
        }
    }

    private static class ProfileRunMeta {
        String runId;
        Instant completedAt;
        String errorMessage;
        String sourceType;
        String schemaName;
        String tableName;

        static final RowMapper<ProfileRunMeta> ROW_MAPPER = (rs, rowNum) -> {
            ProfileRunMeta m = new ProfileRunMeta();
            m.runId = rs.getString("run_id");
            Timestamp ts = rs.getTimestamp("completed_at");
            m.completedAt = ts != null ? ts.toInstant() : null;
            m.errorMessage = rs.getString("error_message");
            m.sourceType = rs.getString("source_type");
            m.schemaName = rs.getString("schema_name");
            m.tableName = rs.getString("table_name");
            return m;
        };
    }

    private static class TableProfileRowMapper implements RowMapper<TableProfile> {
        static final TableProfileRowMapper INSTANCE = new TableProfileRowMapper();

        @Override
        public TableProfile mapRow(ResultSet rs, int rowNum) throws SQLException {
            Timestamp profiledAt = rs.getTimestamp("profiled_at");
            return TableProfile.builder()
                    .sourceType(rs.getString("source_type"))
                    .schemaName(rs.getString("schema_name"))
                    .tableName(rs.getString("table_name"))
                    .rowCountEstimate(rs.getLong("row_count_estimate"))
                    .avgRowSizeBytes(rs.getInt("avg_row_size_bytes"))
                    .estimatedTotalBytes(rs.getLong("estimated_total_bytes"))
                    .complexity(rs.getString("complexity"))
                    .profiledAt(profiledAt != null ? profiledAt.toInstant() : Instant.now())
                    .build();
        }
    }

    private static class ThroughputSampleRowMapper implements RowMapper<ThroughputSample> {
        static final ThroughputSampleRowMapper INSTANCE = new ThroughputSampleRowMapper();

        @Override
        public ThroughputSample mapRow(ResultSet rs, int rowNum) throws SQLException {
            Timestamp sampledAt = rs.getTimestamp("sampled_at");
            return ThroughputSample.builder()
                    .sourceType(rs.getString("source_type"))
                    .schemaName(rs.getString("schema_name"))
                    .tableName(rs.getString("table_name"))
                    .bytesRead(rs.getLong("bytes_read"))
                    .durationMs(rs.getLong("duration_ms"))
                    .rowsRead(rs.getInt("rows_read"))
                    .throughputMbPerSec(rs.getDouble("throughput_mb_per_sec"))
                    .sampledAt(sampledAt != null ? sampledAt.toInstant() : Instant.now())
                    .build();
        }
    }
}
