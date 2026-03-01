package com.di.streamnova.ai.tool;

import com.di.streamnova.handler.jdbc.postgres.DataQualityCheckService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.ai.tool.annotation.ToolParam;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * Spring AI Tool — exposes data quality checks to Gemini so it can detect
 * issues in the source data before running an expensive Dataflow job.
 *
 * <p>Gemini calls this when the user says "check data quality before running"
 * or "are there nulls in the primary key column?".
 */
@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(name = "streamnova.ai.enabled", havingValue = "true", matchIfMissing = true)
public class DataQualityTool {

    private final DataQualityCheckService dataQualityChecker;

    /**
     * Runs a suite of data quality checks on a Postgres table and returns a report.
     */
    @Tool(description = """
            Runs data quality checks on a source table: null counts, duplicate primary keys,
            data type mismatches, and outlier detection.
            Returns a human-readable quality report.
            Currently supports postgres source type.
            """)
    public String runDataQualityCheck(
            @ToolParam(description = "Schema name, e.g. public")   String schema,
            @ToolParam(description = "Table name, e.g. orders")    String tableName,
            @ToolParam(description = "Primary key column name, e.g. id") String primaryKeyColumn
    ) {
        log.info("[TOOL:dq] schema={} table={} pk={}", schema, tableName, primaryKeyColumn);
        try {
            Map<String, Object> report = dataQualityChecker.runChecks(schema, tableName, primaryKeyColumn);
            return formatQualityReport(tableName, report);
        } catch (Exception ex) {
            log.warn("[TOOL:dq] Check failed for {}.{}: {}", schema, tableName, ex.getMessage());
            return "Data quality check failed for " + tableName + ": " + ex.getMessage();
        }
    }

    /**
     * Checks only for null values in specified columns.
     */
    @Tool(description = """
            Checks for NULL values in specific columns of a source table.
            Returns the null count per column.
            """)
    public String checkNullValues(
            @ToolParam(description = "Schema name")             String schema,
            @ToolParam(description = "Table name")              String tableName,
            @ToolParam(description = "Comma-separated column names to check for nulls, e.g. id,name,email")
            String columns
    ) {
        log.info("[TOOL:dq-nulls] schema={} table={} cols={}", schema, tableName, columns);
        try {
            List<String> colList = List.of(columns.split(","));
            Map<String, Long> nullCounts = dataQualityChecker.countNulls(schema, tableName, colList);
            StringBuilder sb = new StringBuilder("Null counts for " + tableName + ":\n");
            nullCounts.forEach((col, count) ->
                    sb.append(String.format("  %-30s %,d nulls%n", col + ":", count)));
            return sb.toString();
        } catch (Exception ex) {
            return "Null check failed: " + ex.getMessage();
        }
    }

    // ------------------------------------------------------------------ //

    private String formatQualityReport(String tableName, Map<String, Object> report) {
        StringBuilder sb = new StringBuilder("Data Quality Report: ").append(tableName).append("\n");
        report.forEach((key, value) ->
                sb.append(String.format("  %-35s %s%n", key + ":", value)));
        return sb.toString();
    }
}
