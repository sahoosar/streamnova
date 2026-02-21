package com.di.streamnova.config;

import com.di.streamnova.handler.SourceHandlerRegistry;
import com.di.streamnova.util.ConnectionPoolLogger;
import com.di.streamnova.util.HikariDataSource;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * At startup, creates a connection pool only for connections that have a registered {@link com.di.streamnova.handler.SourceHandler}.
 * Connection keys without a handler are skipped (no pool created). Pools are created sequentially; then failure/success summary is logged.
 */
@Component
@Order(1)
@RequiredArgsConstructor
@Slf4j
public class DataSourceStartupInitializer implements ApplicationRunner {

    private final PipelineConfigService pipelineConfigService;
    private final SourceHandlerRegistry sourceHandlerRegistry;
    private final PipelineConfigFilesProperties pipelineConfigFilesProperties;

    @Override
    public void run(ApplicationArguments args) {
        if (pipelineConfigFilesProperties != null && pipelineConfigFilesProperties.isUseEventConfigsOnly()) {
            log.info("[DS-STARTUP] use-event-configs-only: true — skipping startup datasource creation. Call POST /api/agent/pipeline-listener/create-datasource (or GET datasource-details) with source/target to create pools, then run pipeline.");
            return;
        }
        pipelineConfigService.logConfigLoadStatusAtStartup();

        if (pipelineConfigService.getEffectiveLoadConfig() == null) {
            log.warn("[DS-STARTUP] Pipeline config not loaded; skipping datasource initialization. Set streamnova.pipeline.config-file and ensure the YAML file exists.");
            return;
        }

        String connectionErrors = pipelineConfigService.getMissingConfigAttributesForConnection();
        if (connectionErrors != null && !connectionErrors.isBlank()) {
            log.error("[DS-STARTUP] Connection config errors — fix the listed config file and pipeline.config.connections: {}", connectionErrors);
        }

        Set<String> connectionKeys = pipelineConfigService.getAvailableSourceKeys();
        if (connectionKeys == null || connectionKeys.isEmpty()) {
            log.warn("[DS-STARTUP] No pipeline.config.connections in config; no pools to create.");
            return;
        }

        // Only create pools for connection types that have a registered handler; skip others
        List<String> toInit = connectionKeys.stream()
                .distinct()
                .filter(key -> sourceHandlerRegistry.hasHandler(key))
                .collect(Collectors.toList());
        List<String> skippedNoHandler = connectionKeys.stream()
                .distinct()
                .filter(key -> !sourceHandlerRegistry.hasHandler(key))
                .collect(Collectors.toList());
        if (!skippedNoHandler.isEmpty()) {
            log.info("[DS-STARTUP] Skipping {} connection(s) with no registered handler: {} (registered handler types: {})",
                    skippedNoHandler.size(), skippedNoHandler, sourceHandlerRegistry.getRegisteredTypes());
        }
        if (toInit.isEmpty()) {
            log.warn("[DS-STARTUP] No connections have a registered handler; no pools to create. Registered handler types: {}",
                    sourceHandlerRegistry.getRegisteredTypes());
            return;
        }

        // When expected-handler is set, create pools only for that handler (execute as per requirement)
        String expectedHandler = pipelineConfigFilesProperties != null ? pipelineConfigFilesProperties.getExpectedHandler() : null;
        if (expectedHandler != null && !expectedHandler.isBlank()) {
            String expected = expectedHandler.trim().toLowerCase();
            List<String> filtered = toInit.stream()
                    .filter(key -> key != null && key.trim().toLowerCase().equals(expected))
                    .collect(Collectors.toList());
            if (filtered.isEmpty()) {
                log.warn("[DS-STARTUP] streamnova.pipeline.expected-handler={} but no matching connection with a handler in {}. Creating no pools. Set defaultSource to {} or add {} to config with a registered handler.",
                        expectedHandler, toInit, expectedHandler, expectedHandler);
                return;
            }
            toInit = filtered;
            log.info("[DS-STARTUP] Expected handler '{}': creating pool(s) only for this handler", expectedHandler.trim());
        }

        ConnectionPoolLogger.logDatasourceSectionStart("Creating datasources sequentially (" + toInit.size() + " connection(s)): " + toInit);
        PoolInitFailureRecorder.clearFailures();

        for (int i = 0; i < toInit.size(); i++) {
            String connectionKey = toInit.get(i);
            int step = i + 1;
            log.info("[DS-STARTUP] Datasource {}/{}: creating connection pool for '{}'", step, toInit.size(), connectionKey);
            initPoolFor(connectionKey);
            if (PoolInitFailureRecorder.getAllFailures().containsKey(connectionKey)) {
                log.warn("[DS-STARTUP] Datasource {}/{}: connection '{}' failed (see error above)", step, toInit.size(), connectionKey);
            } else {
                log.info("[DS-STARTUP] Datasource {}/{}: connection '{}' created successfully", step, toInit.size(), connectionKey);
            }
        }

        Map<String, String> failures = PoolInitFailureRecorder.getAllFailures();
        if (!failures.isEmpty()) {
            log.error("[DS-STARTUP] Datasource creation finished with {} failure(s):", failures.size());
            for (Map.Entry<String, String> e : failures.entrySet()) {
                String configFile = pipelineConfigService.getConfigFileDisplayName(e.getKey());
                log.error("[DS-STARTUP]   - '{}': {}. Check {} pipeline.config.connections.{}", e.getKey(), e.getValue(), configFile, e.getKey());
            }
        }
        List<String> ready = toInit.stream()
                .filter(s -> !failures.containsKey(s))
                .collect(Collectors.toList());
        log.info("[DS-STARTUP] Datasource summary: {} ready ({}), {} failed ({})", ready.size(), ready, failures.size(), new ArrayList<>(failures.keySet()));
        ConnectionPoolLogger.logDatasourceSectionEnd();
    }

    private void initPoolFor(String connectionKey) {
        try {
            PipelineConfigSource src = pipelineConfigService.getEffectiveSourceConfig(connectionKey);
            if (src == null) {
                String configFile = pipelineConfigService.getConfigFileDisplayName(connectionKey);
                log.error("[DS-STARTUP] No config for connection '{}'; skip pool. Check {} pipeline.config.connections.{}", connectionKey, configFile, connectionKey);
                PoolInitFailureRecorder.setLastFailure(connectionKey, "No config for connection key — check " + configFile);
                return;
            }
            // Skip JDBC pool for non-JDBC source types (e.g. gcs); they don't use a connection pool
            if (src.getType() != null && "gcs".equals(src.getType().trim().toLowerCase())) {
                log.info("[DS-STARTUP] Skipping pool for connection '{}' (type=gcs, no JDBC pool needed)", connectionKey);
                return;
            }
            HikariDataSource.INSTANCE.getOrInit(src.toDbConfigSnapshot());
        } catch (Exception e) {
            String configFile = pipelineConfigService.getConfigFileDisplayName(connectionKey);
            PoolInitFailureRecorder.setLastFailure(connectionKey, e.getMessage());
            log.error("[DS-STARTUP] Failed to create pool for connection '{}': {}. Check {} pipeline.config.connections.{} (type, driver, jdbcUrl, username, password, maximumPoolSize, idleTimeout, connectionTimeout, maxLifetime)", connectionKey, e.getMessage(), configFile, connectionKey);
        }
    }
}
