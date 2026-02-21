package com.di.streamnova.config;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Service;

import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

/**
 * Loads pipeline config from event config files only (database_event_config.yml, gcs_event_config.yml, bq_event_config.yml)
 * per user choice for 2-stage or 3-stage. Merges source from one file, optional intermediate from another, destination from another,
 * and returns a single LoadConfig for actions-mapping, datasource creation, and pipeline execute.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class EventConfigLoaderService {

    private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private static final Pattern ENV_PATTERN = Pattern.compile("\\$\\{([^}:]+)(?::([^}]*))?\\}");

    private final ResourceLoader resourceLoader;
    private final PipelineConfigFilesProperties configFilesProperties;

    /**
     * Loads and merges config from event config files for the given selection.
     * 2-stage: sourceKey + targetKey (intermediateKey null). 3-stage: sourceKey + intermediateKey + targetKey.
     *
     * @return merged LoadConfig with source, optional intermediate, and target set; or null if any required fragment fails to load
     */
    public LoadConfig getMergedConfig(String sourceKey, String intermediateKey, String targetKey) {
        if (sourceKey == null || sourceKey.isBlank() || targetKey == null || targetKey.isBlank()) {
            log.warn("Event config: source and target keys are required");
            return null;
        }
        String src = sourceKey.trim().toLowerCase();
        String tgt = targetKey.trim().toLowerCase();
        String mid = (intermediateKey != null && !intermediateKey.isBlank()) ? intermediateKey.trim().toLowerCase() : null;

        PipelineConfigSource sourceConfig = resolveSource(src);
        if (sourceConfig == null) {
            log.warn("Event config: could not resolve source for key '{}'", sourceKey);
            return null;
        }
        IntermediateConfig intermediateConfig = null;
        if (mid != null) {
            intermediateConfig = resolveIntermediate(mid);
            if (intermediateConfig == null) {
                log.warn("Event config: could not resolve intermediate for key '{}'", intermediateKey);
                return null;
            }
        }
        TargetConfig targetConfig = resolveDestination(tgt);
        if (targetConfig == null) {
            log.warn("Event config: could not resolve destination for key '{}'", targetKey);
            return null;
        }

        LoadConfig merged = new LoadConfig();
        // Ensure source type matches request key for handler resolution (e.g. source=gcs → type=gcs; source=bq → type=bigquery)
        if (sourceConfig != null && src != null) {
            if ("gcs".equals(src)) {
                sourceConfig.setType("gcs");
            } else if ("bq".equals(src) || "bigquery".equals(src)) {
                sourceConfig.setType("bigquery");
            }
        }
        merged.setSource(sourceConfig);
        merged.setDefaultSource(src);
        merged.setIntermediate(intermediateConfig);
        if (mid != null) merged.setDefaultIntermediate(mid);
        merged.setTarget(targetConfig);
        merged.setDefaultDestination(tgt);
        merged.setListeners(loadListenersFromClasspath());
        resolveEnvVarsInConfig(merged);
        return merged;
    }

    /** Loads one event config file and returns the raw LoadConfig (partial ok). */
    public LoadConfig loadFragment(String handlerKey) {
        String path = configFilesProperties.getEventConfigFilePath(handlerKey);
        if (path == null || path.isBlank()) {
            log.debug("Event config: no path for handler '{}'", handlerKey);
            return null;
        }
        try {
            Resource resource = resourceLoader.getResource(path.trim());
            if (!resource.exists()) {
                log.warn("Event config: file not found for '{}': {}", handlerKey, path);
                return null;
            }
            try (InputStream in = resource.getInputStream()) {
                PipelineYamlRoot root = YAML_MAPPER.readValue(in, PipelineYamlRoot.class);
                if (root == null || root.getPipeline() == null || root.getPipeline().getConfig() == null) {
                    return null;
                }
                LoadConfig config = root.getPipeline().getConfig();
                resolveEnvVarsInConfig(config);
                return config;
            }
        } catch (Exception e) {
            log.warn("Event config: failed to load for '{}' from {}: {}", handlerKey, path, e.getMessage());
            return null;
        }
    }

    private PipelineConfigSource resolveSource(String sourceKey) {
        LoadConfig fragment = loadFragment(sourceKey);
        if (fragment == null) return null;
        Map<String, PipelineConfigSource> sourceMap = getSourcesMap(fragment);
        if (sourceMap != null) {
            if (sourceMap.containsKey(sourceKey)) {
                return sourceMap.get(sourceKey);
            }
            // Aliases: API may send source=bq but YAML has sources.bigquery; source=gcs but YAML has sources.gcs_inputs
            String alias = sourceKeyAliasForSource(sourceKey);
            if (alias != null && sourceMap.containsKey(alias)) {
                return sourceMap.get(alias);
            }
        }
        return fragment.getSource();
    }

    /** Resolves source key to the config key used in event YAML (e.g. bq→bigquery, gcs→gcs_inputs). */
    private static String sourceKeyAliasForSource(String sourceKey) {
        if (sourceKey == null) return null;
        switch (sourceKey) {
            case "bq": return "bigquery";
            case "bigquery": return "bq";
            case "gcs": return "gcs_inputs";
            default: return null;
        }
    }

    private IntermediateConfig resolveIntermediate(String intermediateKey) {
        LoadConfig fragment = loadFragment(intermediateKey);
        if (fragment == null) return null;
        if (fragment.getIntermediates() != null) {
            if (fragment.getIntermediates().containsKey(intermediateKey)) {
                return fragment.getIntermediates().get(intermediateKey);
            }
            // gcs_event_config.yml uses intermediates.gcs_stage; API passes intermediate=gcs
            if ("gcs".equals(intermediateKey) && fragment.getIntermediates().containsKey("gcs_stage")) {
                return fragment.getIntermediates().get("gcs_stage");
            }
        }
        return fragment.getIntermediate();
    }

    private TargetConfig resolveDestination(String targetKey) {
        LoadConfig fragment = loadFragment(targetKey);
        if (fragment == null) return null;
        if (fragment.getDestinations() != null) {
            if (fragment.getDestinations().containsKey(targetKey)) {
                return fragment.getDestinations().get(targetKey);
            }
            // API may send target=bq but YAML has destinations.bigquery
            String alias = targetKeyAliasForDestination(targetKey);
            if (alias != null && fragment.getDestinations().containsKey(alias)) {
                return fragment.getDestinations().get(alias);
            }
        }
        return fragment.getTarget();
    }

    /** Resolves target key to the config key used in event YAML (e.g. bq→bigquery). */
    private static String targetKeyAliasForDestination(String targetKey) {
        if (targetKey == null) return null;
        if ("bq".equals(targetKey)) return "bigquery";
        if ("bigquery".equals(targetKey)) return "bq";
        return null;
    }

    private static Map<String, PipelineConfigSource> getSourcesMap(LoadConfig config) {
        if (config == null) return null;
        if (config.getSources() != null && !config.getSources().isEmpty()) return config.getSources();
        return config.getConnections();
    }

    /** Loads listeners from the configured listener config file (environment-specific when set in application-{profile}.yml). */
    private Map<String, StageActionsConfig> loadListenersFromClasspath() {
        String path = configFilesProperties != null ? configFilesProperties.getListenerConfigFilePath() : "classpath:pipeline_listener_config.yml";
        try {
            Resource resource = resourceLoader.getResource(path);
            if (!resource.exists()) return null;
            try (InputStream in = resource.getInputStream()) {
                PipelineYamlRoot root = YAML_MAPPER.readValue(in, PipelineYamlRoot.class);
                if (root == null || root.getPipeline() == null || root.getPipeline().getConfig() == null) return null;
                LoadConfig c = root.getPipeline().getConfig();
                return c.getListeners();
            }
        } catch (Exception e) {
            log.debug("Could not load listener config {}: {}", path, e.getMessage());
            return null;
        }
    }

    private void resolveEnvVarsInConfig(LoadConfig config) {
        if (config == null) return;
        if (config.getSource() != null && config.getSource().getPassword() != null) {
            config.getSource().setPassword(resolveEnvVars(config.getSource().getPassword()));
        }
        Map<String, PipelineConfigSource> sourceMap = getSourcesMap(config);
        if (sourceMap != null) {
            sourceMap.values().forEach(src -> {
                if (src != null && src.getPassword() != null) {
                    src.setPassword(resolveEnvVars(src.getPassword()));
                }
            });
        }
    }

    private static String resolveEnvVars(String value) {
        if (value == null || value.isBlank()) return value;
        Matcher m = ENV_PATTERN.matcher(value);
        StringBuffer sb = new StringBuffer();
        while (m.find()) {
            String varName = m.group(1);
            String defaultVal = m.group(2);
            String envVal = System.getenv(varName);
            m.appendReplacement(sb, Matcher.quoteReplacement(envVal != null ? envVal : (defaultVal != null ? defaultVal : "")));
        }
        m.appendTail(sb);
        return sb.toString();
    }
}
