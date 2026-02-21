package com.di.streamnova.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.HashMap;
import java.util.Map;

/**
 * Profile-based pipeline config file paths per connection (from application.yml / application-{profile}.yml).
 * When {@link #useEventConfigsOnly} is true, only {@link #eventConfigFiles} are used (database_event_config.yml, gcs_event_config.yml, bq_event_config.yml).
 * <p>
 * When {@link #expectedHandler} is set, the pipeline runs only when the resolved source type
 * matches this handler (e.g. postgres, oracle). Use to enforce a particular handler per environment.
 */
@Data
@ConfigurationProperties(prefix = "streamnova.pipeline")
public class PipelineConfigFilesProperties {

    /** Map of connection name -> config file path. Ignored when useEventConfigsOnly=true (event configs used instead). */
    private Map<String, String> configFiles = new HashMap<>();

    /**
     * When true, pipeline config is loaded only from event config files (database_event_config.yml, gcs_event_config.yml, bq_event_config.yml)
     * per user choice (2-stage or 3-stage). Config comes from event config files only.
     */
    private boolean useEventConfigsOnly = false;

    /**
     * Event config file path per handler key. E.g. postgres -> classpath:database_event_config.yml, gcs -> classpath:gcs_event_config.yml, bigquery -> classpath:bq_event_config.yml.
     * Used when useEventConfigsOnly=true for actions-mapping, datasource-details, and pipeline execute.
     */
    private Map<String, String> eventConfigFiles = new HashMap<>();

    /**
     * Listener config file path (entrypoint for 2-stage / 3-stage actions). Set per environment in application-{profile}.yml.
     * E.g. classpath:pipeline_listener_config.yml (default), classpath:pipeline_listener_config-sit.yml, pipeline_listener_config-prod.yml.
     * When empty, falls back to classpath:pipeline_listener_config.yml.
     */
    private String listenerConfigFile = "";

    /**
     * Expected handler type for this run (e.g. postgres, oracle).
     * When set, the pipeline executes only when the resolved source matches this handler.
     * Empty/blank = no restriction (any registered handler allowed).
     */
    private String expectedHandler = "";

    /** Resolved listener config path: configured value or default classpath. */
    public String getListenerConfigFilePath() {
        if (listenerConfigFile != null && !listenerConfigFile.isBlank()) {
            return listenerConfigFile.trim();
        }
        return "classpath:pipeline_listener_config.yml";
    }

    public String getConfigFilePath(String connectionKey) {
        if (connectionKey == null) return null;
        if (useEventConfigsOnly && eventConfigFiles != null) {
            String path = eventConfigFiles.get(connectionKey.toLowerCase());
            if (path != null && !path.isBlank()) return path;
        }
        if (configFiles == null) return null;
        return configFiles.get(connectionKey.toLowerCase());
    }

    /** Path to event config file for the given handler key (e.g. postgres, gcs, bigquery). Supports bigquery/bq alias. */
    public String getEventConfigFilePath(String handlerKey) {
        if (handlerKey == null) return null;
        String key = handlerKey.trim().toLowerCase();
        if (eventConfigFiles != null) {
            String path = eventConfigFiles.get(key);
            if (path != null && !path.isBlank()) return path;
            if ("bigquery".equals(key) || "bq".equals(key)) {
                path = "bigquery".equals(key) ? eventConfigFiles.get("bq") : eventConfigFiles.get("bigquery");
                if (path != null && !path.isBlank()) return path;
            }
        }
        if ("bigquery".equals(key) || "bq".equals(key)) return "classpath:bq_event_config.yml";
        return null;
    }

    /** Display name for logs (filename part of path). */
    public String getConfigFileDisplayName(String connectionKey) {
        String path = getConfigFilePath(connectionKey);
        if (path == null || path.isBlank()) return connectionKey != null ? connectionKey + "_pipeline_config.yml" : "pipeline_config.yml";
        int last = Math.max(path.lastIndexOf('/'), path.lastIndexOf(':'));
        return last >= 0 ? path.substring(last + 1).trim() : path.trim();
    }

    /** Returns classpath path for fallback when primary is file: path; null if not a classpath path. */
    public String getClasspathPathForConnection(String connectionKey) {
        String path = getConfigFilePath(connectionKey);
        if (path == null || path.isBlank() || !path.trim().toLowerCase().startsWith("classpath")) return null;
        return path.trim();
    }
}
