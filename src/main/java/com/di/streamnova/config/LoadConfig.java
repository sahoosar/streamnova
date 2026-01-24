package com.di.streamnova.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

@Data
public class LoadConfig {
    @Valid
    @NotNull
    private PipelineConfigSource source;
    @Valid
    @NotNull
    private IntermediateConfig intermediate;
    @Valid
    @NotNull
    private TargetConfig target;
}
