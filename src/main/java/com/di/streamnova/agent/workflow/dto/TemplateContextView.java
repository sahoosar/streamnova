package com.di.streamnova.agent.workflow.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Safe view of template context for REST response (password masked).
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class TemplateContextView {

    private String runId;
    private String datePartition;
    private String sourceKey;
    private String sourceType;
    private String driverClass;
    private String jdbcUrl;
    private String username;
    /** Always masked in response. */
    private String password;
    private String sourceSchema;
    private String sourceTable;
    private String sourceQuery;
    private Integer fetchSize;
    private Integer maxDbConnections;
    private Integer minimumIdle;
    private Long connectionTimeoutMs;
    private Long idleTimeoutMs;
    private Long maxLifetimeMs;
    private Integer workers;
    private Integer shards;
    private String extractionMode;
    private String incrementalColumn;
    private String watermarkFrom;
    private String watermarkTo;
    private String defaultBytesPerRowBytes;
    private Boolean enableSourceRowCount;
    private String sourceCountQuery;
    private String outputBucket;
    private String outputBasePath;
    private String fileFormat;
    private String compression;
    private Integer targetFileSizeMb;
    private String writeMode;
    private Boolean traceEnabled;

    /** Builds a view from context with password masked. */
    public static TemplateContextView from(DbToGcsTemplateContext ctx) {
        if (ctx == null) return null;
        return TemplateContextView.builder()
                .runId(ctx.getRunId())
                .datePartition(ctx.getDatePartition())
                .sourceKey(ctx.getSourceKey())
                .sourceType(ctx.getSourceType())
                .driverClass(ctx.getDriverClass())
                .jdbcUrl(ctx.getJdbcUrl())
                .username(ctx.getUsername())
                .password(ctx.getPassword() != null && !ctx.getPassword().isEmpty() ? "***" : null)
                .sourceSchema(ctx.getSourceSchema())
                .sourceTable(ctx.getSourceTable())
                .sourceQuery(ctx.getSourceQuery())
                .fetchSize(ctx.getFetchSize())
                .maxDbConnections(ctx.getMaxDbConnections())
                .minimumIdle(ctx.getMinimumIdle())
                .connectionTimeoutMs(ctx.getConnectionTimeoutMs())
                .idleTimeoutMs(ctx.getIdleTimeoutMs())
                .maxLifetimeMs(ctx.getMaxLifetimeMs())
                .workers(ctx.getWorkers())
                .shards(ctx.getShards())
                .extractionMode(ctx.getExtractionMode())
                .incrementalColumn(ctx.getIncrementalColumn())
                .watermarkFrom(ctx.getWatermarkFrom())
                .watermarkTo(ctx.getWatermarkTo())
                .defaultBytesPerRowBytes(ctx.getDefaultBytesPerRowBytes())
                .enableSourceRowCount(ctx.isEnableSourceRowCount())
                .sourceCountQuery(ctx.getSourceCountQuery())
                .outputBucket(ctx.getOutputBucket())
                .outputBasePath(ctx.getOutputBasePath())
                .fileFormat(ctx.getFileFormat())
                .compression(ctx.getCompression())
                .targetFileSizeMb(ctx.getTargetFileSizeMb())
                .writeMode(ctx.getWriteMode())
                .traceEnabled(ctx.isTraceEnabled())
                .build();
    }
}
