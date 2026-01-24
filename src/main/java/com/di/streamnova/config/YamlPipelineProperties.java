package com.di.streamnova.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import lombok.Data;
import org.springframework.validation.annotation.Validated;
@Data
@Validated
@ConfigurationProperties(prefix = "pipeline")
public class YamlPipelineProperties {
    @Valid
    @NotNull
    private LoadConfig config;
}