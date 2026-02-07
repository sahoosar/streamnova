package com.di.streamnova.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

import java.util.Map;

@Data
public class LoadConfig {
    /** Single source when one DB (e.g. only postgres or only oracle profile). */
    @Valid
    private PipelineConfigSource source;

    /** Named sources when both profiles active (postgres,oracle); keys match supported-source-types. */
    private Map<String, @Valid PipelineConfigSource> sources;

    private String defaultSource;

    /**
     * Optional named scenarios (e.g. light, heavy) to support different machine types or users.
     * Select via streamnova.pipeline.scenario=light or env STREAMNOVA_PIPELINE_SCENARIO=heavy.
     * When set, the matching scenario's overrides are applied to all sources.
     */
    private Map<String, ScenarioOverrides> scenarios;
    /** Default scenario name when streamnova.pipeline.scenario is not set. */
    private String defaultScenario;

    @Valid
    @NotNull
    private IntermediateConfig intermediate;
    @Valid
    @NotNull
    private TargetConfig target;
}
