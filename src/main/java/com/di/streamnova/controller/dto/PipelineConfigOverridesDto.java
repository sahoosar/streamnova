package com.di.streamnova.controller.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Optional runtime overrides for pipeline.config.source.
 * Only non-null fields are applied over the YAML config.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PipelineConfigOverridesDto {
    private String table;
    private String jdbcUrl;
    private String username;
    private String password;
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
}
