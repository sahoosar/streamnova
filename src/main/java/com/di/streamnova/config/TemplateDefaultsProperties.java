package com.di.streamnova.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Default values for pipeline template context when event config (TargetConfig) does not set them.
 * Configure in application.yml under streamnova.pipeline.template-defaults.
 * <p>
 * {@link #targetFileSizeMb} is a target hint only; actual output file sizes may vary.
 * Use 0 or leave unset to omit (runner uses its own default when the writer is implemented).
 */
@Data
@ConfigurationProperties(prefix = "streamnova.pipeline.template-defaults")
public class TemplateDefaultsProperties {

    private String fileFormat = "PARQUET";
    private String compression = "SNAPPY";
    /** Target size per output file (MB). Optional; 0 = no target (use runner default). Actual sizes may vary. */
    private int targetFileSizeMb = 64;
    private String writeMode = "APPEND";
    private boolean traceEnabled = true;
}
