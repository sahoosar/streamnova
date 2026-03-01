package com.di.streamnova.load.metadata;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.util.List;
import java.util.Optional;

/**
 * JDBC repository for the {@code load_validations} table.
 */
@Repository
@RequiredArgsConstructor
public class LoadValidationRepository {

    private final JdbcTemplate jdbc;

    private static final RowMapper<LoadValidation> ROW_MAPPER = (rs, n) -> {
        LoadValidation v = new LoadValidation();
        v.setId(rs.getString("id"));
        v.setRunId(rs.getString("run_id"));
        v.setStage(rs.getString("stage"));
        long exp = rs.getLong("expected_rows");
        v.setExpectedRows(rs.wasNull() ? null : exp);
        long act = rs.getLong("actual_rows");
        v.setActualRows(rs.wasNull() ? null : act);
        long delta = rs.getLong("delta_rows");
        v.setDeltaRows(rs.wasNull() ? null : delta);
        double dpct = rs.getDouble("delta_pct");
        v.setDeltaPct(rs.wasNull() ? null : dpct);
        double thr = rs.getDouble("threshold_pct");
        v.setThresholdPct(rs.wasNull() ? null : thr);
        v.setPassed(rs.getBoolean("passed"));
        v.setDetail(rs.getString("detail"));
        Timestamp va = rs.getTimestamp("validated_at");
        v.setValidatedAt(va == null ? null : va.toInstant());
        return v;
    };

    public void insert(LoadValidation val) {
        jdbc.update("""
            INSERT INTO load_validations
              (id, run_id, stage, expected_rows, actual_rows, delta_rows,
               delta_pct, threshold_pct, passed, detail)
            VALUES (?,?,?,?,?,?,?,?,?,?)
            """,
            val.getId(), val.getRunId(), val.getStage(),
            val.getExpectedRows(), val.getActualRows(), val.getDeltaRows(),
            val.getDeltaPct(), val.getThresholdPct(), val.isPassed(), val.getDetail());
    }

    public Optional<LoadValidation> findByRunAndStage(String runId, String stage) {
        List<LoadValidation> rows = jdbc.query(
                "SELECT * FROM load_validations WHERE run_id = ? AND stage = ?",
                ROW_MAPPER, runId, stage);
        return rows.isEmpty() ? Optional.empty() : Optional.of(rows.get(0));
    }

    public List<LoadValidation> findByRunId(String runId) {
        return jdbc.query(
                "SELECT * FROM load_validations WHERE run_id = ? ORDER BY validated_at",
                ROW_MAPPER, runId);
    }
}
