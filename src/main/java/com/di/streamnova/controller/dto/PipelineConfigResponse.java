package com.di.streamnova.controller.dto;

import com.di.streamnova.config.PipelineConfigSource;
import lombok.Builder;
import lombok.Data;

import java.util.List;

/**
 * Read-only view of effective pipeline source config for REST. Password is masked.
 */
@Data
@Builder
public class PipelineConfigResponse {
    private String type;
    private String driver;
    private String jdbcUrl;
    private String username;
    private String passwordMasked;  // "***" if set, never the real password
    private String table;
    private Integer fetchSize;
    private String upperBoundColumn;
    private String partitionValue;
    private String shardColumn;
    private List<String> orderBy;
    private Integer maximumPoolSize;
    private Integer minimumIdle;
    private Integer queryTimeout;
    private Integer statementTimeout;
    private Boolean enableProgressLogging;
    private Integer shards;
    private Integer workers;
    private String machineType;
    private Double fetchFactor;
    /** True if runtime overrides are currently applied. */
    private boolean runtimeOverridesActive;

    public static PipelineConfigResponse from(PipelineConfigSource source, boolean runtimeOverridesActive) {
        if (source == null) {
            return null;
        }
        return PipelineConfigResponse.builder()
                .type(source.getType())
                .driver(source.getDriver())
                .jdbcUrl(source.getJdbcUrl())
                .username(source.getUsername())
                .passwordMasked(source.getPassword() != null && !source.getPassword().isEmpty() ? "***" : null)
                .table(source.getTable())
                .fetchSize(source.getFetchSize() > 0 ? source.getFetchSize() : null)
                .upperBoundColumn(source.getUpperBoundColumn())
                .partitionValue(source.getPartitionValue())
                .shardColumn(source.getShardColumn())
                .orderBy(source.getOrderBy())
                .maximumPoolSize(source.getMaximumPoolSize() > 0 ? source.getMaximumPoolSize() : null)
                .minimumIdle(source.getMinimumIdle() > 0 ? source.getMinimumIdle() : null)
                .queryTimeout(source.getQueryTimeout())
                .statementTimeout(source.getStatementTimeout())
                .enableProgressLogging(source.getEnableProgressLogging())
                .shards(source.getShards())
                .workers(source.getWorkers())
                .machineType(source.getMachineType())
                .fetchFactor(source.getFetchFactor())
                .runtimeOverridesActive(runtimeOverridesActive)
                .build();
    }
}
