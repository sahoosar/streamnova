package com.di.streamnova.config;

import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Optional overrides for pipeline source config, keyed by scenario name (e.g. "light", "heavy").
 * Use when different users or machine types need different load settings in the same YAML.
 * Select via: streamnova.pipeline.scenario=light or env STREAMNOVA_PIPELINE_SCENARIO=heavy.
 * Only non-null / set values override the base source config.
 */
@Data
@NoArgsConstructor
public class ScenarioOverrides {
    private Integer fetchSize;
    private Double fetchFactor;
    private Integer maximumPoolSize;
    private Integer fallbackPoolSize;
    private Integer minimumIdle;
    private Integer shards;
    private Integer workers;
    private String machineType;
    private String defaultBytesPerRow;

    /**
     * Applies this scenario's overrides onto the given source. Only non-null/non-zero values are applied.
     */
    public void applyTo(PipelineConfigSource source) {
        if (source == null) return;
        if (fetchSize != null && fetchSize > 0) source.setFetchSize(fetchSize);
        if (fetchFactor != null) source.setFetchFactor(fetchFactor);
        if (maximumPoolSize != null && maximumPoolSize > 0) source.setMaximumPoolSize(maximumPoolSize);
        if (fallbackPoolSize != null) source.setFallbackPoolSize(fallbackPoolSize);
        if (minimumIdle != null && minimumIdle >= 0) source.setMinimumIdle(minimumIdle);
        if (shards != null) source.setShards(shards);
        if (workers != null) source.setWorkers(workers);
        if (machineType != null && !machineType.isBlank()) source.setMachineType(machineType.trim());
        if (defaultBytesPerRow != null && !defaultBytesPerRow.isBlank()) source.setDefaultBytesPerRow(defaultBytesPerRow);
    }
}
