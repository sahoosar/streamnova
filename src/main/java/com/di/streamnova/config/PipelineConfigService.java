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
 * Resolves effective pipeline config from the file at streamnova.pipeline.config-file only.
 * No in-memory config: all attributes (password, jdbcUrl, table, etc.) are loaded from the config file
 * on each request. Password can use env vars (e.g. ${POSTGRES_PASSWORD}) and is resolved at read time.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class PipelineConfigService {

    private final ResourceLoader resourceLoader;

    private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    @Value("${streamnova.pipeline.default-source:}")
    private String defaultSourceOverride;

    /**
     * Optional scenario name (e.g. light, heavy) to apply machine-type or user-specific overrides from pipeline.config.scenarios.
     * Can also be set via env STREAMNOVA_PIPELINE_SCENARIO. When empty, defaultScenario from YAML is used if set.
     */
    @Value("${streamnova.pipeline.scenario:}")
    private String scenarioOverride;

    /**
     * Optional path to pipeline config file (e.g. file:./postgre_pipeline_config.yml).
     * When set, config is re-read from this file on each request so password changes take effect without restart.
     */
    @Value("${streamnova.pipeline.config-file:}")
    private String configFilePath;

    @Value("${streamnova.statistics.supported-source-types}")
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

    public LoadConfig getEffectiveLoadConfig(String sourceKey) {
        LoadConfig yamlConfig = getConfig();
        if (yamlConfig == null) return null;
        PipelineConfigSource effectiveSource = getEffectiveSourceConfig(sourceKey);
        if (effectiveSource == null) return yamlConfig;
        LoadConfig merged = new LoadConfig();
        merged.setSource(effectiveSource);
        merged.setIntermediate(yamlConfig.getIntermediate());
        merged.setTarget(yamlConfig.getTarget());
        return merged;
    }

    public Set<String> getAvailableSourceKeys() {
        LoadConfig config = getConfig();
        if (config == null || config.getSources() == null || config.getSources().isEmpty()) {
            return Collections.emptySet();
        }
        return Collections.unmodifiableSet(config.getSources().keySet());
    }

    public boolean isMultiSource() {
        LoadConfig config = getConfig();
        return config != null && config.getSources() != null && !config.getSources().isEmpty();
    }

    /**
     * Returns a clear reason why no connection pools exist (for pool-statistics when pools are empty).
     * If any required config attributes are missing, returns "Config attributes missing for datasource connection: &lt;list&gt;".
     */
    public String getReasonNoPools(String lastPoolInitFailure) {
        if (configFilePath == null || configFilePath.isBlank()) {
            return "streamnova.pipeline.config-file is not set. Set it in application-<db>.properties (e.g. application-postgres.properties: streamnova.pipeline.config-file=file:src/main/resources/postgre_pipeline_config.yml).";
        }
        LoadConfig config = getConfig();
        if (config == null) {
            return "Pipeline config file could not be loaded. Check that the path is correct (if using file:, run from project root), the file exists, and the YAML has pipeline.config.sources with jdbcUrl, username, and password.";
        }
        String missingAttrs = getMissingConfigAttributesForConnection(config);
        if (missingAttrs != null && !missingAttrs.isBlank()) {
            return "Config attributes missing for datasource connection: " + missingAttrs;
        }
        if (lastPoolInitFailure != null && !lastPoolInitFailure.isBlank()) {
            if (lastPoolInitFailure.contains("is required") || lastPoolInitFailure.contains("pipeline.config.source")) {
                return "Config attributes missing for datasource connection: " + lastPoolInitFailure;
            }
            return "Pipeline config was loaded but connection pool creation failed at startup: " + lastPoolInitFailure;
        }
        return "Pipeline config is loaded but no connection pool was created. Check startup logs for connection errors (e.g. invalid username or password, database unreachable). Ensure pipeline.config.source.password is set in the config file.";
    }

    /**
     * Returns a comma-separated list of missing config attribute names (per source if multi-source), or null if none missing.
     * Uses the provided config so the caller's loaded config is used (single load).
     */
    public String getMissingConfigAttributesForConnection(LoadConfig config) {
        if (config == null) return null;
        List<String> parts = new ArrayList<>();
        if (config.getSources() != null && !config.getSources().isEmpty()) {
            for (Map.Entry<String, PipelineConfigSource> e : config.getSources().entrySet()) {
                if (e.getValue() == null) {
                    parts.add(e.getKey() + ": (source config null)");
                } else {
                    List<String> missing = e.getValue().getMissingAttributesForConnection();
                    if (!missing.isEmpty()) {
                        parts.add(e.getKey() + ": " + String.join(", ", missing));
                    }
                }
            }
        } else if (config.getSource() != null) {
            List<String> missing = config.getSource().getMissingAttributesForConnection();
            if (!missing.isEmpty()) {
                parts.add(String.join(", ", missing));
            }
        } else {
            return "no source configured (add pipeline.config.sources.<name> in pipeline_config.yml, e.g. sources.postgres with type, driver, jdbcUrl, username, password, table, maximumPoolSize, idleTimeout, connectionTimeout, maxLifetime)";
        }
        return parts.isEmpty() ? null : String.join("; ", parts);
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
     * Returns the pipeline config by loading from the file(s) at streamnova.pipeline.config-file.
     * When streamnova.statistics.supported-source-types contains both "postgres" and "oracle", loads both
     * postgre_pipeline_config.yml and oracle_pipeline_config.yml and merges sources so both DBs appear in config.
     */
    private LoadConfig getConfig() {
        if (configFilePath == null || configFilePath.isBlank()) {
            if (!configFileRequiredLogged) {
                configFileRequiredLogged = true;
                log.warn("Pipeline config-file is not set. Set streamnova.pipeline.config-file (e.g. file:src/main/resources/postgre_pipeline_config.yml). All config is loaded from file only.");
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
        if (config.getSources() != null) {
            config.getSources().values().forEach(overrides::applyTo);
            log.info("Applied pipeline scenario '{}' overrides to {} source(s)", scenario, config.getSources().size());
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
        boolean needOracle = supportedTypes.contains("oracle") && (primary.getSources() == null || !primary.getSources().containsKey("oracle"));
        boolean needPostgres = supportedTypes.contains("postgres") && (primary.getSources() == null || !primary.getSources().containsKey("postgres"));
        if (needOracle) {
            LoadConfig other = loadOtherConfig(primaryPath, true);
            if (other != null && other.getSources() != null && other.getSources().containsKey("oracle")) {
                Map<String, PipelineConfigSource> merged = new LinkedHashMap<>();
                if (primary.getSources() != null) primary.getSources().forEach(merged::put);
                merged.put("oracle", other.getSources().get("oracle"));
                primary.setSources(merged);
                log.debug("Merged pipeline config: added oracle source from oracle_pipeline_config.yml");
            } else {
                log.warn("Could not load oracle source: oracle_pipeline_config.yml not found or invalid. Set streamnova.statistics.supported-source-types=postgres to suppress.");
            }
        }
        if (needPostgres) {
            LoadConfig other = loadOtherConfig(primaryPath, false);
            if (other != null && other.getSources() != null && other.getSources().containsKey("postgres")) {
                Map<String, PipelineConfigSource> merged = new LinkedHashMap<>();
                if (primary.getSources() != null) primary.getSources().forEach(merged::put);
                merged.put("postgres", other.getSources().get("postgres"));
                primary.setSources(merged);
                log.debug("Merged pipeline config: added postgres source from postgre_pipeline_config.yml");
            } else {
                log.warn("Could not load postgres source: postgre_pipeline_config.yml not found or invalid.");
            }
        }
        return primary;
    }

    /** Load the other config (oracle if loadOracle, else postgres). Tries classpath then file path. */
    private LoadConfig loadOtherConfig(String primaryPath, boolean loadOracle) {
        String fileName = loadOracle ? "oracle_pipeline_config.yml" : "postgre_pipeline_config.yml";
        LoadConfig loaded = loadConfigFromFile("classpath:/" + fileName, false);
        if (loaded == null) loaded = loadConfigFromFile("classpath:" + fileName, false);
        if (loaded == null) loaded = loadConfigFromFile("file:src/main/resources/" + fileName, false);
        return loaded;
    }


    private static List<String> parseSupportedTypes(String config) {
        if (config == null || config.isBlank()) return List.of("postgres");
        return Arrays.stream(config.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .map(String::toLowerCase)
                .distinct()
                .collect(Collectors.toList());
    }

    /** If path looks like file:.../src/main/resources/foo.yml, return classpath:foo.yml for fallback. */
    private String toClasspathPath(String path) {
        if (path == null) return null;
        if (path.contains("postgre_pipeline_config")) return "classpath:postgre_pipeline_config.yml";
        if (path.contains("oracle_pipeline_config")) return "classpath:oracle_pipeline_config.yml";
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

    /** Resolves ${VAR} and ${VAR:default} in password fields from environment variables. No hardcoded passwords. */
    private void resolveEnvVarsInConfig(LoadConfig config) {
        if (config == null) return;
        if (config.getSource() != null && config.getSource().getPassword() != null) {
            config.getSource().setPassword(resolveEnvVars(config.getSource().getPassword()));
        }
        if (config.getSources() != null) {
            config.getSources().values().forEach(src -> {
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

    private PipelineConfigSource resolveBaseSource(LoadConfig config, String sourceKey) {
        Map<String, PipelineConfigSource> sources = config.getSources();
        if (sources != null && !sources.isEmpty()) {
            String key = sourceKey != null && !sourceKey.isBlank() ? sourceKey.trim()
                    : (defaultSourceOverride != null && !defaultSourceOverride.isBlank() ? defaultSourceOverride.trim() : config.getDefaultSource());
            if (key == null || key.isBlank()) key = sources.keySet().iterator().next();
            return sources.get(key);
        }
        return config.getSource();
    }
}
