package com.di.streamnova.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Loads and resolves pipeline config from YAML.
 * <ul>
 *   <li>When use-event-configs-only: config is built from event config files (database_event_config.yml, gcs_event_config.yml, bq_event_config.yml) via EventConfigLoaderService.</li>
 *   <li>Otherwise: config is read from streamnova.pipeline.config-file; credentials can use env vars (e.g. ${POSTGRES_PASSWORD}).</li>
 *   <li>Source resolution: ?source=key can be a connection name (e.g. postgres) or a table key (e.g. market_summary); table key merges connection + pipeline.config.tables.&lt;key&gt;.</li>
 * </ul>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class PipelineConfigService {

    private final ResourceLoader resourceLoader;
    private final PipelineConfigFilesProperties configFilesProperties;

    private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    /**
     * Optional scenario name (e.g. light, heavy) to apply machine-type or user-specific overrides from pipeline.config.scenarios.
     * Can also be set via env STREAMNOVA_PIPELINE_SCENARIO. When empty, defaultScenario from YAML is used if set.
     */
    @Value("${streamnova.pipeline.scenario:}")
    private String scenarioOverride;

    /**
     * Optional path to pipeline config file (e.g. file:./pipeline_config.yml).
     * When set, config is re-read from this file on each request so password changes take effect without restart. Ignored when use-event-configs-only=true.
     */
    @Value("${streamnova.pipeline.config-file:}")
    private String configFilePath;

    @Value("${streamnova.statistics.supported-source-types:postgres}")
    private String supportedSourceTypesConfig;

    /** Log once when config-file is not set. */
    private static volatile boolean configFileRequiredLogged;
    /** Log once when loading from file. */
    private static volatile boolean fileConfigUsageLogged;

    public PipelineConfigSource getEffectiveSourceConfig() {
        return getEffectiveSourceConfig(null);
    }

    public PipelineConfigSource getEffectiveSourceConfig(String sourceKey) {
        LoadConfig config = getConfig();
        if (config == null) return null;
        PipelineConfigSource base = resolveBaseSource(config, sourceKey);
        return base;
    }

    public LoadConfig getEffectiveLoadConfig() {
        return getEffectiveLoadConfig(null);
    }

    /**
     * Returns effective load config with resolved source, intermediate, and destination.
     * When YAML uses named handler-type configs (sources/intermediates/destinations), the selected config per stage is resolved.
     * Returns null only when config file could not be loaded.
     */
    public LoadConfig getEffectiveLoadConfig(String sourceKey) {
        return getEffectiveLoadConfig(sourceKey, null, null);
    }

    /**
     * Returns effective load config with resolved source, intermediate, and destination.
     * User can select which config to use per stage via keys (e.g. source=oracle, intermediate=gcs, target=bigquery).
     * When a key is provided and the corresponding map exists, that entry is used; otherwise defaults are used.
     */
    public LoadConfig getEffectiveLoadConfig(String sourceKey, String intermediateKey, String targetKey) {
        LoadConfig yamlConfig = getConfig();
        if (yamlConfig == null) return null;
        PipelineConfigSource effectiveSource = getEffectiveSourceConfig(sourceKey);
        IntermediateConfig effectiveIntermediate = resolveEffectiveIntermediate(yamlConfig, intermediateKey);
        TargetConfig effectiveDestination = resolveEffectiveDestination(yamlConfig, targetKey);
        LoadConfig merged = new LoadConfig();
        merged.setIntermediate(effectiveIntermediate);
        merged.setTarget(effectiveDestination);
        merged.setSource(effectiveSource != null ? effectiveSource : yamlConfig.getSource());
        return merged;
    }

    /** Resolves the single intermediate config. When overrideKey is null/blank, returns null (2-stage). When set, resolves from intermediates map or legacy. */
    private IntermediateConfig resolveEffectiveIntermediate(LoadConfig config, String overrideKey) {
        if (overrideKey == null || overrideKey.isBlank()) {
            return null;
        }
        if (config.getIntermediates() != null && !config.getIntermediates().isEmpty()) {
            String key = overrideKey.trim();
            IntermediateConfig c = config.getIntermediates().get(key);
            if (c != null) return c;
        }
        return config.getIntermediate();
    }

    /** Resolves the single destination config: by override key, or defaultDestination, or legacy target. */
    private TargetConfig resolveEffectiveDestination(LoadConfig config, String overrideKey) {
        if (config.getDestinations() != null && !config.getDestinations().isEmpty()) {
            String key = overrideKey != null && !overrideKey.isBlank() ? overrideKey.trim()
                    : (config.getDefaultDestination() != null && !config.getDefaultDestination().isBlank()
                            ? config.getDefaultDestination().trim()
                            : config.getDestinations().keySet().iterator().next());
            TargetConfig c = config.getDestinations().get(key);
            if (c != null) return c;
        }
        return config.getTarget();
    }

    /** Returns the map used for source resolution: sources when set, else connections (legacy). */
    private Map<String, PipelineConfigSource> getSourcesMap(LoadConfig config) {
        if (config == null) return null;
        if (config.getSources() != null && !config.getSources().isEmpty()) {
            return config.getSources();
        }
        return config.getConnections();
    }

    /**
     * Resolves pipeline handler requirements from config: source (e.g. oracle), intermediate (e.g. gcs), target (e.g. bigquery).
     * Used at execution start so the pipeline knows and executes accordingly.
     *
     * @param loadConfig effective load config (with resolved source); if null, returns null
     * @return requirements, or null if loadConfig is null or source is null
     */
    public PipelineHandlerRequirements getPipelineHandlerRequirements(LoadConfig loadConfig) {
        if (loadConfig == null || loadConfig.getSource() == null) {
            return null;
        }
        String sourceType = loadConfig.getSource().getType();
        if (sourceType == null || sourceType.isBlank()) {
            return null;
        }
        String intermediateType = null;
        if (loadConfig.getIntermediate() != null) {
            intermediateType = loadConfig.getIntermediate().getEffectiveHandlerType();
        }
        String targetType = null;
        if (loadConfig.getTarget() != null && loadConfig.getTarget().getType() != null && !loadConfig.getTarget().getType().isBlank()) {
            targetType = loadConfig.getTarget().getType().trim().toLowerCase();
        }
        return PipelineHandlerRequirements.builder()
                .sourceHandlerType(sourceType.trim().toLowerCase())
                .intermediateHandlerType(intermediateType)
                .targetHandlerType(targetType)
                .build();
    }

    public Set<String> getAvailableSourceKeys() {
        LoadConfig config = getConfig();
        Map<String, PipelineConfigSource> sourceMap = getSourcesMap(config);
        if (config == null || sourceMap == null || sourceMap.isEmpty()) {
            return Collections.emptySet();
        }
        return Collections.unmodifiableSet(sourceMap.keySet());
    }

    public boolean isMultiSource() {
        LoadConfig config = getConfig();
        Map<String, PipelineConfigSource> sourceMap = getSourcesMap(config);
        return config != null && sourceMap != null && !sourceMap.isEmpty();
    }

    /** Returns configured source config keys (for agent/UI: user selects one as source). */
    public Set<String> getConfiguredSourceKeys() {
        return getAvailableSourceKeys();
    }

    /** Returns configured intermediate config keys (for agent/UI: user selects one for 3-stage). */
    public Set<String> getConfiguredIntermediateKeys() {
        LoadConfig config = getConfig();
        if (config == null || config.getIntermediates() == null || config.getIntermediates().isEmpty()) {
            return Collections.emptySet();
        }
        return Collections.unmodifiableSet(config.getIntermediates().keySet());
    }

    /** Returns configured destination config keys (for agent/UI: user selects one as target). */
    public Set<String> getConfiguredDestinationKeys() {
        LoadConfig config = getConfig();
        if (config == null || config.getDestinations() == null || config.getDestinations().isEmpty()) {
            return Collections.emptySet();
        }
        return Collections.unmodifiableSet(config.getDestinations().keySet());
    }

    /** Returns the default source key from config (defaultTable or defaultSource). Used to build execute response selection when user omits source. */
    public String getDefaultSourceKey() {
        LoadConfig config = getConfig();
        if (config == null) return null;
        if (config.getDefaultTable() != null && !config.getDefaultTable().isBlank()) {
            return config.getDefaultTable();
        }
        return config.getDefaultSource();
    }

    /** Returns the default destination key from config. Used to build execute response selection when user omits target. */
    public String getDefaultDestinationKey() {
        LoadConfig config = getConfig();
        if (config == null) return null;
        return config.getDefaultDestination();
    }

    /** Returns the default intermediate key from config (for 3-stage when user omits intermediate). */
    public String getDefaultIntermediateKey() {
        LoadConfig config = getConfig();
        if (config == null) return null;
        return config.getDefaultIntermediate();
    }

    /**
     * Returns the list of pipeline actions (and event types) for the given stage mode.
     * Uses pipeline.config.listeners.2-stage / pipeline.config.listeners.3-stage when present; otherwise defaults.
     */
    public List<PipelineActionConfig> getListenerActionsForMode(boolean twoStage) {
        LoadConfig config = getConfig();
        String key = twoStage ? "2-stage" : "3-stage";
        if (config != null && config.getListeners() != null) {
            StageActionsConfig stage = config.getListeners().get(key);
            if (stage != null && stage.getActions() != null && !stage.getActions().isEmpty()) {
                return new ArrayList<>(stage.getActions());
            }
        }
        return defaultListenerActionsForMode(twoStage);
    }

    private static List<PipelineActionConfig> defaultListenerActionsForMode(boolean twoStage) {
        List<PipelineActionConfig> list = new ArrayList<>();
        if (twoStage) {
            list.add(actionConfig("SOURCE_READ", "LOAD_SOURCE_READ"));
            list.add(actionConfig("TARGET_WRITE", "LOAD_TARGET_WRITE"));
        } else {
            list.add(actionConfig("SOURCE_READ", "LOAD_SOURCE_READ"));
            list.add(actionConfig("INTERMEDIATE_WRITE", "LOAD_INTERMEDIATE_WRITE"));
            list.add(actionConfig("TARGET_WRITE", "LOAD_TARGET_WRITE"));
        }
        return list;
    }

    private static PipelineActionConfig actionConfig(String action, String eventType) {
        PipelineActionConfig c = new PipelineActionConfig();
        c.setAction(action);
        c.setEventType(eventType);
        return c;
    }

    /**
     * Returns a clear reason why no connection pools exist (for pool-statistics when pools are empty).
     * If any required config attributes are missing, returns "Config attributes missing for datasource connection: &lt;list&gt;".
     */
    public String getReasonNoPools(String lastPoolInitFailure) {
        if (configFilePath == null || configFilePath.isBlank()) {
            return "streamnova.pipeline.config-file is not set. Set it in application-<profile>.yml (e.g. streamnova.pipeline.config-file: classpath:pipeline_config.yml), or use event configs only with source/target from API.";
        }
        LoadConfig config = getConfig();
        if (config == null) {
            return "Pipeline config file could not be loaded. Check that the path is correct (if using file:, run from project root), the file exists, and the YAML has pipeline.config.sources or pipeline.config.connections with jdbcUrl, username, and password.";
        }
        String missingAttrs = getMissingConfigAttributesForConnection(config);
        if (missingAttrs != null && !missingAttrs.isBlank()) {
            return "Connection config errors (fix the listed config file and connection): " + missingAttrs;
        }
        if (lastPoolInitFailure != null && !lastPoolInitFailure.isBlank()) {
            if (lastPoolInitFailure.contains("is required") || lastPoolInitFailure.contains("pipeline.config")) {
                return "Connection config errors or pool creation failed: " + lastPoolInitFailure;
            }
            return "Pipeline config was loaded but connection pool creation failed at startup: " + lastPoolInitFailure;
        }
        return "Pipeline config is loaded but no connection pool was created. Check startup logs for connection errors (e.g. invalid username or password, database unreachable). Ensure pipeline.config.source.password is set in the config file.";
    }

    /**
     * Returns connection config errors (missing or invalid attributes) for the current loaded config.
     * Includes which config file to check per connection.
     */
    public String getMissingConfigAttributesForConnection() {
        LoadConfig config = getConfig();
        return getMissingConfigAttributesForConnection(config);
    }

    /**
     * Returns a comma-separated list of missing config attribute names (per connection), or null if none missing.
     * Each connection's errors include the config file to check.
     */
    public String getMissingConfigAttributesForConnection(LoadConfig config) {
        if (config == null) return null;
        Map<String, PipelineConfigSource> sourceMap = getSourcesMap(config);
        List<String> parts = new ArrayList<>();
        String section = config.getSources() != null && !config.getSources().isEmpty() ? "sources" : "connections";
        if (sourceMap != null && !sourceMap.isEmpty()) {
            for (Map.Entry<String, PipelineConfigSource> e : sourceMap.entrySet()) {
                String connName = e.getKey();
                String configFile = configFileNameForConnection(connName);
                if (e.getValue() == null) {
                    parts.add(connName + ": (source config null) — check " + configFile + " pipeline.config." + section + "." + connName);
                } else {
                    List<String> missing = e.getValue().getMissingAttributesForConnection();
                    if (!missing.isEmpty()) {
                        parts.add(connName + ": missing or invalid [" + String.join(", ", missing) + "] — check " + configFile + " pipeline.config." + section + "." + connName);
                    }
                }
            }
        } else if (config.getSource() != null) {
            List<String> missing = config.getSource().getMissingAttributesForConnection();
            if (!missing.isEmpty()) {
                parts.add("missing or invalid [" + String.join(", ", missing) + "] — check pipeline config file pipeline.config." + section);
            }
        } else {
            return "no source configured (add pipeline.config.sources.<name> or pipeline.config.connections.<name> in the config file(s); include type, driver, jdbcUrl, username, password, maximumPoolSize, idleTimeout, connectionTimeout, maxLifetime)";
        }
        return parts.isEmpty() ? null : String.join("; ", parts);
    }

    /** Returns the config file display name for a given connection (from streamnova.pipeline.config-files). */
    public String getConfigFileDisplayName(String connectionKey) {
        return configFilesProperties != null ? configFilesProperties.getConfigFileDisplayName(connectionKey) : "pipeline_config.yml";
    }

    private String configFileNameForConnection(String connectionKey) {
        return getConfigFileDisplayName(connectionKey);
    }

    /**
     * Call at startup to log whether the pipeline config file was loaded. Use from an ApplicationRunner with @Order(0).
     */
    public void logConfigLoadStatusAtStartup() {
        if (configFilePath == null || configFilePath.isBlank()) {
            log.warn("[STARTUP] Pipeline config file: NOT loaded (streamnova.pipeline.config-file not set)");
            return;
        }
        String path = configFilePath.trim();
        LoadConfig loaded = loadConfigFromFile(path, true);
        if (loaded == null && path.startsWith("classpath:") && !path.startsWith("classpath:/")) {
            loaded = loadConfigFromFile("classpath:/" + path.substring("classpath:".length()), false);
        }
        if (loaded == null) {
            String classpathPath = toClasspathPath(path);
            if (classpathPath != null && !classpathPath.equals(path)) {
                loaded = loadConfigFromFile(classpathPath, false);
            }
        }
        if (loaded != null) {
            log.info("[STARTUP] Pipeline config file: loaded successfully from {}", path);
        } else {
            log.warn("[STARTUP] Pipeline config file: NOT loaded from {} (file missing or invalid). Cwd: {}", path, System.getProperty("user.dir"));
        }
    }

    /**
     * Returns the pipeline config by loading from the file at streamnova.pipeline.config-file.
     * When supported-source-types contains multiple types, may merge from multiple config files per config-files map.
     */
    private LoadConfig getConfig() {
        if (configFilePath == null || configFilePath.isBlank()) {
            if (!configFileRequiredLogged) {
                configFileRequiredLogged = true;
                if (configFilesProperties == null || !configFilesProperties.isUseEventConfigsOnly()) {
                    log.warn("Pipeline config-file is not set. Set streamnova.pipeline.config-file (e.g. classpath:pipeline_config.yml). All config is loaded from file only when not using event-configs-only.");
                }
            }
            return null;
        }
        String path = configFilePath.trim();
        LoadConfig loaded = loadConfigFromFile(path, true);
        if (loaded == null) {
            loaded = tryClasspathFallback(path);
            if (loaded == null) return null;
        }

        List<String> supportedTypes = parseSupportedTypes(supportedSourceTypesConfig);
        if (supportedTypes.size() >= 2 && supportedTypes.contains("postgres") && supportedTypes.contains("oracle")) {
            loaded = mergeWithOtherSourceConfig(loaded, path, supportedTypes);
        }
        applyScenarioOverrides(loaded);
        return loaded;
    }

    /**
     * If streamnova.pipeline.scenario (or config's defaultScenario) is set and pipeline.config.scenarios contains that key,
     * applies the scenario's overrides to all sources so different users/machine types can share one YAML.
     */
    private void applyScenarioOverrides(LoadConfig config) {
        if (config == null) return;
        String scenario = scenarioOverride != null && !scenarioOverride.isBlank()
                ? scenarioOverride.trim()
                : (config.getDefaultScenario() != null && !config.getDefaultScenario().isBlank()
                        ? config.getDefaultScenario().trim()
                        : null);
        if (scenario == null) return;
        Map<String, ScenarioOverrides> scenarios = config.getScenarios();
        if (scenarios == null || !scenarios.containsKey(scenario)) {
            if (scenarios != null && !scenarios.isEmpty()) {
                log.debug("Pipeline scenario '{}' not found in config.scenarios (available: {}). Ignoring.", scenario, scenarios.keySet());
            }
            return;
        }
        ScenarioOverrides overrides = scenarios.get(scenario);
        if (overrides == null) return;
        Map<String, PipelineConfigSource> sourceMap = getSourcesMap(config);
        if (sourceMap != null) {
            sourceMap.values().forEach(overrides::applyTo);
            log.info("Applied pipeline scenario '{}' overrides to {} source(s)", scenario, sourceMap.size());
        }
        if (config.getSource() != null) {
            overrides.applyTo(config.getSource());
            log.info("Applied pipeline scenario '{}' overrides to single source", scenario);
        }
    }

    private LoadConfig tryClasspathFallback(String path) {
        if (path.startsWith("file:")) {
            log.warn("Pipeline config-file path not readable. Path tried: {} | Cwd: {}", path, System.getProperty("user.dir"));
            return null;
        }
        if (path.startsWith("classpath:") && !path.startsWith("classpath:/")) {
            LoadConfig loaded = loadConfigFromFile("classpath:/" + path.substring("classpath:".length()), false);
            if (loaded != null) return loaded;
        }
        String classpathPath = toClasspathPath(path);
        if (classpathPath != null && !classpathPath.equals(path)) {
            LoadConfig loaded = loadConfigFromFile(classpathPath, false);
            if (loaded != null) {
                log.warn("Pipeline config loaded from classpath fallback '{}', NOT from configured path '{}'.", classpathPath, path);
                return loaded;
            }
        }
        log.warn("Pipeline config re-read from '{}' failed. Cwd: {}", path, System.getProperty("user.dir"));
        return null;
    }

    /** When both postgres and oracle are supported, load the other config file and merge sources. */
    private LoadConfig mergeWithOtherSourceConfig(LoadConfig primary, String primaryPath, List<String> supportedTypes) {
        Map<String, PipelineConfigSource> primarySources = getSourcesMap(primary);
        boolean needOracle = supportedTypes.contains("oracle") && (primarySources == null || !primarySources.containsKey("oracle"));
        boolean needPostgres = supportedTypes.contains("postgres") && (primarySources == null || !primarySources.containsKey("postgres"));
        if (needOracle) {
            String oracleFile = getConfigFileDisplayName("oracle");
            LoadConfig other = loadOtherConfig("oracle");
            Map<String, PipelineConfigSource> otherSources = getSourcesMap(other);
            if (other != null && otherSources != null && otherSources.containsKey("oracle")) {
                Map<String, PipelineConfigSource> merged = new LinkedHashMap<>();
                if (primarySources != null) primarySources.forEach(merged::put);
                merged.put("oracle", otherSources.get("oracle"));
                if (primary.getSources() != null) primary.setSources(merged);
                else primary.setConnections(merged);
                log.info("[CONFIG] Pipeline config: oracle connection loaded from {}", oracleFile);
            } else {
                log.error("[CONFIG] Oracle connection missing or invalid: {} not found or has no pipeline.config.sources.oracle / connections.oracle. Fix {} (type, driver, jdbcUrl, username, password, maximumPoolSize, idleTimeout, connectionTimeout, maxLifetime) or set streamnova.statistics.supported-source-types=postgres to use only postgres.", oracleFile, oracleFile);
            }
        }
        if (needPostgres) {
            String postgresFile = getConfigFileDisplayName("postgres");
            LoadConfig other = loadOtherConfig("postgres");
            Map<String, PipelineConfigSource> otherSources = getSourcesMap(other);
            if (other != null && otherSources != null && otherSources.containsKey("postgres")) {
                Map<String, PipelineConfigSource> merged = new LinkedHashMap<>();
                primarySources = getSourcesMap(primary);
                if (primarySources != null) primarySources.forEach(merged::put);
                merged.put("postgres", otherSources.get("postgres"));
                if (primary.getSources() != null) primary.setSources(merged);
                else primary.setConnections(merged);
                log.info("[CONFIG] Pipeline config: postgres connection loaded from {}", postgresFile);
            } else {
                log.error("[CONFIG] Postgres connection missing or invalid: {} not found or has no pipeline.config.sources.postgres / connections.postgres. Fix {} (type, driver, jdbcUrl, username, password, maximumPoolSize, idleTimeout, connectionTimeout, maxLifetime).", postgresFile, postgresFile);
            }
        }
        return primary;
    }

    /** Load the config for the given connection (from streamnova.pipeline.config-files). Tries configured path then classpath/file fallbacks. */
    private LoadConfig loadOtherConfig(String connectionKey) {
        String path = configFilesProperties.getConfigFilePath(connectionKey);
        if (path != null && !path.isBlank()) {
            LoadConfig loaded = loadConfigFromFile(path.trim(), false);
            if (loaded != null) return loaded;
            if (path.trim().startsWith("classpath:") && !path.trim().startsWith("classpath:/")) {
                loaded = loadConfigFromFile("classpath:/" + path.trim().substring("classpath:".length()), false);
                if (loaded != null) return loaded;
            }
        }
        String displayName = configFilesProperties.getConfigFileDisplayName(connectionKey);
        LoadConfig loaded = loadConfigFromFile("classpath:/" + displayName, false);
        if (loaded == null) loaded = loadConfigFromFile("classpath:" + displayName, false);
        if (loaded == null) loaded = loadConfigFromFile("file:src/main/resources/" + displayName, false);
        return loaded;
    }


    private static List<String> parseSupportedTypes(String config) {
        if (config == null || config.isBlank()) {
            return List.of("postgres");
        }
        return Arrays.stream(config.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .map(String::toLowerCase)
                .distinct()
                .collect(Collectors.toList());
    }

    /** If path matches a configured connection's file, return that connection's classpath path for fallback. */
    private String toClasspathPath(String path) {
        if (path == null || configFilesProperties == null || configFilesProperties.getConfigFiles() == null) return null;
        for (String connectionKey : configFilesProperties.getConfigFiles().keySet()) {
            String classpathPath = configFilesProperties.getClasspathPathForConnection(connectionKey);
            if (classpathPath != null) {
                String fileName = configFilesProperties.getConfigFileDisplayName(connectionKey);
                if (path.contains(fileName)) return classpathPath;
            }
        }
        return null;
    }

    private LoadConfig loadConfigFromFile(String location, boolean logSuccess) {
        try {
            InputStream in = openConfigInputStream(location);
            if (in == null) return null;
            try (in) {
                PipelineYamlRoot root = YAML_MAPPER.readValue(in, PipelineYamlRoot.class);
                if (root == null || root.getPipeline() == null || root.getPipeline().getConfig() == null) {
                    log.warn("Pipeline config from {} has missing pipeline.config structure", location);
                    return null;
                }
                LoadConfig config = root.getPipeline().getConfig();
                resolveEnvVarsInConfig(config);
                if (!validateLoadConfig(config, location)) {
                    return null;
                }
                if (logSuccess && !fileConfigUsageLogged) {
                    fileConfigUsageLogged = true;
                    log.info("Pipeline config loaded from file (password from env vars / file, re-read on each request): {}", location);
                }
                log.debug("Pipeline config re-read from {} (credentials resolved from env/file)", location);
                return config;
            }
        } catch (Exception e) {
            log.warn("Failed to load pipeline config from {}: {} - {}", location, e.getClass().getSimpleName(), e.getMessage());
            return null;
        }
    }

    /** For file: paths, read directly from filesystem so we always get current content (no caching). */
    private InputStream openConfigInputStream(String location) throws Exception {
        if (location != null && location.startsWith("file:")) {
            String pathPart = location.substring(5).trim();
            File file = pathPart.startsWith("/") ? new File(URI.create(location)) : new File(System.getProperty("user.dir"), pathPart);
            if (file.isFile() && file.canRead()) {
                return new FileInputStream(file);
            }
            log.debug("Pipeline config file not found or not readable: {} (resolved to absolute: {})", location, file.getAbsolutePath());
            return null;
        }
        Resource resource = resourceLoader.getResource(location);
        return resource.getInputStream();
    }

    /**
     * Validates required pipeline config structure after load. Supports both legacy (single intermediate/target)
     * and segregated handler-type configs (intermediates/destinations + defaults).
     */
    private boolean validateLoadConfig(LoadConfig config, String location) {
        if (config == null) return false;
        boolean valid = true;
        Map<String, PipelineConfigSource> sourceMap = getSourcesMap(config);
        if (sourceMap == null || sourceMap.isEmpty()) {
            if (config.getSource() == null) {
                log.warn("Pipeline config from {}: no sources/connections and no legacy source", location);
                valid = false;
            }
        }
        boolean hasIntermediate = config.getIntermediate() != null
                || (config.getIntermediates() != null && !config.getIntermediates().isEmpty());
        if (!hasIntermediate) {
            log.warn("Pipeline config from {}: intermediate (staging) is missing (set pipeline.config.intermediate or pipeline.config.intermediates)", location);
            valid = false;
        }
        boolean hasDestination = config.getTarget() != null
                || (config.getDestinations() != null && !config.getDestinations().isEmpty());
        if (!hasDestination) {
            log.warn("Pipeline config from {}: target (destination) is missing (set pipeline.config.target or pipeline.config.destinations)", location);
            valid = false;
        }
        return valid;
    }

    /** Resolves ${VAR} and ${VAR:default} in password fields from environment variables. No hardcoded passwords. */
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

    private static final Pattern ENV_PATTERN = Pattern.compile("\\$\\{([^}:]+)(?::([^}]*))?\\}");

    /** Replaces ${VAR} with System.getenv(VAR), ${VAR:default} with env or default. */
    private String resolveEnvVars(String value) {
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

    /**
     * Resolves the effective source config from config only. No in-code defaults.
     * When sourceKey is null or blank: use config.defaultTable then config.defaultSource (if set in YAML).
     * If still no key, returns null. When key is in config.tables, merges that table with its connection.
     */
    private PipelineConfigSource resolveBaseSource(LoadConfig config, String sourceKey) {
        Map<String, PipelineConfigSource> sourceMap = getSourcesMap(config);
        if (sourceMap == null || sourceMap.isEmpty()) {
            return config.getSource();
        }
        String key = sourceKey != null && !sourceKey.isBlank() ? sourceKey.trim() : null;
        if (key == null || key.isBlank()) {
            key = config.getDefaultTable() != null && !config.getDefaultTable().isBlank() ? config.getDefaultTable().trim()
                    : config.getDefaultSource();
            if (key != null) key = key.trim();
        }
        if (key == null || key.isBlank()) {
            log.debug("No source key and no defaultSource/defaultTable in config; resolution returns null");
            return null;
        }

        Map<String, TableConfig> tables = config.getTables();
        if (tables != null && tables.containsKey(key)) {
            TableConfig tableConfig = tables.get(key);
            if (tableConfig == null) {
                log.warn("Table config '{}' is null in config; cannot resolve", key);
                return null;
            }
            String connName = tableConfig.getConnection();
            if (connName == null || connName.isBlank()) {
                log.warn("Table config '{}' has no connection in config; cannot resolve", key);
                return null;
            }
            PipelineConfigSource connection = sourceMap.get(connName.trim());
            if (connection == null) {
                log.warn("Table config '{}' references connection '{}' which is not in pipeline.config.sources/connections; available: {}", key, connName, sourceMap.keySet());
                return null;
            }
            if (log.isDebugEnabled()) {
                log.debug("Resolved source by table key '{}' (connection '{}', table '{}')", key, connName, tableConfig.getTable());
            }
            return mergeConnectionAndTable(connection, tableConfig);
        }

        PipelineConfigSource resolved = sourceMap.get(key);
        if (resolved == null) {
            log.warn("Source key '{}' not found in config (not a connection or table key); available: {}", key, sourceMap.keySet());
        } else if (log.isDebugEnabled()) {
            log.debug("Resolved source by connection key '{}'", key);
        }
        return resolved;
    }

    /**
     * Merges a connection (DB-level: JDBC, pool, timeouts) with a table config (table name, maxColumns, shardColumn, etc.).
     * Connection fields are copied first; table fields override only when non-null.
     */
    private static PipelineConfigSource mergeConnectionAndTable(PipelineConfigSource connection, TableConfig table) {
        PipelineConfigSource merged = new PipelineConfigSource();
        merged.setType(connection.getType());
        merged.setDriver(connection.getDriver());
        merged.setJdbcUrl(connection.getJdbcUrl());
        merged.setUsername(connection.getUsername());
        merged.setPassword(connection.getPassword());
        merged.setFetchSize(connection.getFetchSize());
        merged.setFetchFactor(connection.getFetchFactor());
        merged.setMaximumPoolSize(connection.getMaximumPoolSize());
        merged.setFallbackPoolSize(connection.getFallbackPoolSize());
        merged.setMinimumIdle(connection.getMinimumIdle());
        merged.setIdleTimeout(connection.getIdleTimeout());
        merged.setConnectionTimeout(connection.getConnectionTimeout());
        merged.setMaxLifetime(connection.getMaxLifetime());
        merged.setQueryTimeout(connection.getQueryTimeout());
        merged.setSocketTimeout(connection.getSocketTimeout());
        merged.setStatementTimeout(connection.getStatementTimeout());
        merged.setEnableProgressLogging(connection.getEnableProgressLogging());
        merged.setShards(connection.getShards());
        merged.setWorkers(connection.getWorkers());
        merged.setMachineType(connection.getMachineType());
        merged.setDefaultBytesPerRow(connection.getDefaultBytesPerRow());
        if (table.getTable() != null && !table.getTable().isBlank()) merged.setTable(table.getTable());
        else merged.setTable(connection.getTable());
        if (table.getMaxColumns() != null) merged.setMaxColumns(table.getMaxColumns());
        else merged.setMaxColumns(connection.getMaxColumns());
        if (table.getUpperBoundColumn() != null) merged.setUpperBoundColumn(table.getUpperBoundColumn());
        else merged.setUpperBoundColumn(connection.getUpperBoundColumn());
        if (table.getPartitionValue() != null) merged.setPartitionValue(table.getPartitionValue());
        else merged.setPartitionValue(connection.getPartitionValue());
        if (table.getShardColumn() != null) merged.setShardColumn(table.getShardColumn());
        else merged.setShardColumn(connection.getShardColumn());
        if (table.getOrderBy() != null) merged.setOrderBy(table.getOrderBy());
        else merged.setOrderBy(connection.getOrderBy());
        return merged;
    }
}
