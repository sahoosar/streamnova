package com.di.streamnova.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

/**
 * Root structure for pipeline YAML files (pipeline.config.*).
 * Used when re-reading config from an external file so password changes take effect without restart.
 */
@Data
public class PipelineYamlRoot {
    @JsonProperty("pipeline")
    private PipelineConfigHolder pipeline;

    @Data
    public static class PipelineConfigHolder {
        @JsonProperty("config")
        private LoadConfig config;
    }
}
