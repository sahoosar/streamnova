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

    @Valid
    @NotNull
    private IntermediateConfig intermediate;
    @Valid
    @NotNull
    private TargetConfig target;
}
