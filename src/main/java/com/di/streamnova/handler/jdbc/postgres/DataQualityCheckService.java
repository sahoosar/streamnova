package com.di.streamnova.handler.jdbc.postgres;

import com.di.streamnova.config.PipelineConfigService;
import com.di.streamnova.util.HikariDataSource;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Service that runs data quality checks using the default pipeline source connection.
 * Used by the AI DataQualityTool.
 */
@Slf4j
@Service
@RequiredArgsConstructor
@ConditionalOnProperty(name = "streamnova.ai.enabled", havingValue = "true", matchIfMissing = true)
public class DataQualityCheckService {

    private final PipelineConfigService pipelineConfigService;

    /**
     * Runs a suite of data quality checks and returns a report map.
     */
    public Map<String, Object> runChecks(String schema, String tableName, String primaryKeyColumn) {
        Map<String, Object> report = new HashMap<>();
        var config = pipelineConfigService.getEffectiveSourceConfig(null);
        if (config == null || !"postgres".equalsIgnoreCase(config.getType())) {
            report.put("error", "No Postgres source config available");
            return report;
        }
        try (Connection conn = HikariDataSource.INSTANCE.getOrInit(config.toDbConfigSnapshot()).getConnection()) {
            List<String> columnsToCheck = primaryKeyColumn != null && !primaryKeyColumn.isBlank()
                    ? List.of(primaryKeyColumn)
                    : List.of();
            List<String> nonNullColumns = PostgresDataQualityChecker.checkNonNullColumnsInData(
                    conn, schema != null ? schema : "public", tableName, columnsToCheck);
            report.put("primary_key_column", primaryKeyColumn);
            report.put("columns_without_nulls", String.join(", ", nonNullColumns));
            report.put("status", "ok");
        } catch (Exception e) {
            log.warn("Data quality check failed for {}.{}: {}", schema, tableName, e.getMessage());
            report.put("error", e.getMessage());
        }
        return report;
    }

    /**
     * Returns null counts per column for the given table.
     */
    public Map<String, Long> countNulls(String schema, String tableName, List<String> columnNames) {
        Map<String, Long> result = new HashMap<>();
        var config = pipelineConfigService.getEffectiveSourceConfig(null);
        if (config == null || !"postgres".equalsIgnoreCase(config.getType())) {
            return result;
        }
        if (columnNames == null || columnNames.isEmpty()) return result;
        String quotedTable = "public".equals(schema) ? "\"" + tableName + "\"" : "\"" + schema + "\".\"" + tableName + "\"";
        try (Connection conn = HikariDataSource.INSTANCE.getOrInit(config.toDbConfigSnapshot()).getConnection()) {
            for (String col : columnNames) {
                String quotedCol = "\"" + col.trim() + "\"";
                String sql = "SELECT COUNT(*) AS c FROM " + quotedTable + " WHERE " + quotedCol + " IS NULL";
                try (PreparedStatement ps = conn.prepareStatement(sql); ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) result.put(col.trim(), rs.getLong("c"));
                }
            }
        } catch (Exception e) {
            log.warn("countNulls failed for {}.{}: {}", schema, tableName, e.getMessage());
        }
        return result;
    }
}
