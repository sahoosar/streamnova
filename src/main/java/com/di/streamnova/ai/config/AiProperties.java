package com.di.streamnova.ai.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;
import org.springframework.stereotype.Component;

/**
 * Strongly-typed binding for all {@code streamnova.ai.*} properties.
 *
 * <p>Change any value in the relevant {@code application-{profile}.yml} — no code changes needed.
 *
 * <pre>
 * streamnova:
 *   ai:
 *     enabled: true
 *     vertex:
 *       project-id: my-gcp-project
 *       location:   us-central1
 *     chat:
 *       model:             gemini-2.0-flash-001
 *       temperature:       0.3
 *       max-output-tokens: 8192
 *     agent:
 *       max-iterations:   10
 *       timeout-seconds:  60
 *       retry-attempts:   3
 *       memory-enabled:   true
 *       memory-max-turns: 20
 * </pre>
 */
@Data
@Component
@ConfigurationProperties(prefix = "streamnova.ai")
public class AiProperties {

    /** Master switch — set to false to disable all AI features without removing the dependency. */
    private boolean enabled = true;

    @NestedConfigurationProperty
    private VertexConfig vertex = new VertexConfig();

    @NestedConfigurationProperty
    private ChatConfig chat = new ChatConfig();

    @NestedConfigurationProperty
    private EmbeddingConfig embedding = new EmbeddingConfig();

    @NestedConfigurationProperty
    private AgentConfig agent = new AgentConfig();

    // ------------------------------------------------------------------ //

    @Data
    public static class VertexConfig {
        private String projectId = "your-gcp-project-id";
        private String location  = "us-central1";
    }

    @Data
    public static class ChatConfig {
        private String model           = "gemini-2.0-flash-001";
        private double temperature     = 0.3;
        private double topP            = 0.95;
        private int    maxOutputTokens = 8192;
    }

    @Data
    public static class EmbeddingConfig {
        private String model              = "text-embedding-004";
        private int    outputDimensionality = 768;
    }

    @Data
    public static class AgentConfig {
        private int     maxIterations  = 10;
        private int     timeoutSeconds = 60;
        private int     retryAttempts  = 3;
        private boolean memoryEnabled  = true;
        private int     memoryMaxTurns = 20;
    }
}
