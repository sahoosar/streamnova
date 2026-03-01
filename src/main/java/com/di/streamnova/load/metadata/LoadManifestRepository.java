package com.di.streamnova.load.metadata;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.util.List;
import java.util.Optional;

/**
 * JDBC repository for the {@code load_manifests} table.
 */
@Repository
@RequiredArgsConstructor
public class LoadManifestRepository {

    private final JdbcTemplate jdbc;

    private static final RowMapper<LoadManifest> ROW_MAPPER = (rs, n) -> {
        LoadManifest m = new LoadManifest();
        m.setId(rs.getString("id"));
        m.setRunId(rs.getString("run_id"));
        m.setStage(rs.getString("stage"));
        m.setManifestGcsPath(rs.getString("manifest_gcs_path"));
        int tf = rs.getInt("total_files");
        m.setTotalFiles(rs.wasNull() ? null : tf);
        long tr = rs.getLong("total_rows");
        m.setTotalRows(rs.wasNull() ? null : tr);
        long tb = rs.getLong("total_bytes");
        m.setTotalBytes(rs.wasNull() ? null : tb);
        Timestamp ca = rs.getTimestamp("created_at");
        m.setCreatedAt(ca == null ? null : ca.toInstant());
        return m;
    };

    public void insert(LoadManifest manifest) {
        jdbc.update("""
            INSERT INTO load_manifests
              (id, run_id, stage, manifest_gcs_path, total_files, total_rows, total_bytes)
            VALUES (?,?,?,?,?,?,?)
            """,
            manifest.getId(), manifest.getRunId(), manifest.getStage(),
            manifest.getManifestGcsPath(), manifest.getTotalFiles(),
            manifest.getTotalRows(), manifest.getTotalBytes());
    }

    public Optional<LoadManifest> findByRunAndStage(String runId, String stage) {
        List<LoadManifest> rows = jdbc.query(
                "SELECT * FROM load_manifests WHERE run_id = ? AND stage = ?",
                ROW_MAPPER, runId, stage);
        return rows.isEmpty() ? Optional.empty() : Optional.of(rows.get(0));
    }

    public List<LoadManifest> findByRunId(String runId) {
        return jdbc.query(
                "SELECT * FROM load_manifests WHERE run_id = ? ORDER BY created_at",
                ROW_MAPPER, runId);
    }
}
