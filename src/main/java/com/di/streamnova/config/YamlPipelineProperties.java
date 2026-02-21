package com.di.streamnova.config;

import jakarta.validation.Valid;
import org.springframework.boot.context.properties.ConfigurationProperties;
import lombok.Data;
import org.springframework.validation.annotation.Validated;

/**
 * Optional binding for pipeline.config from Spring property sources (e.g. application.yml or imported YAML).
 * Pipeline config is primarily loaded from file by {@link PipelineConfigService}; this bean allows
 * pipeline.config to be absent without binding failure when only file-based config is used.
 */
@Data
@Validated
@ConfigurationProperties(prefix = "pipeline")
public class YamlPipelineProperties {
    @Valid
    private LoadConfig config;
}