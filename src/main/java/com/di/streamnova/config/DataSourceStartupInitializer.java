package com.di.streamnova.config;

import com.di.streamnova.util.HikariDataSource;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * At startup, reads {@code streamnova.statistics.supported-source-types} (e.g. "postgres, oracle"),
 * picks up config for each type from the merged pipeline config (application-postgres + application-oracle when both profiles active),
 * and creates datasources in parallel for both databases.
 */
@Component
@Order(1)
@RequiredArgsConstructor
@Slf4j
public class DataSourceStartupInitializer implements ApplicationRunner {

    private final PipelineConfigService pipelineConfigService;

    @Value("${streamnova.statistics.supported-source-types:postgres}")
    private String supportedSourceTypesConfig;

    @Override
    public void run(ApplicationArguments args) {
        pipelineConfigService.logConfigLoadStatusAtStartup();

        List<String> types = parseSupportedTypes(supportedSourceTypesConfig);
        if (types.isEmpty()) return;

        if (pipelineConfigService.isMultiSource()) {
            Set<String> available = pipelineConfigService.getAvailableSourceKeys();
            List<String> toInit = types.stream()
                    .filter(available::contains)
                    .distinct()
                    .collect(Collectors.toList());
            if (toInit.isEmpty()) {
                log.debug("[STARTUP] No source types from supported-source-types match config sources {}", available);
                return;
            }
            log.info("[STARTUP] Creating datasources in parallel for: {}", toInit);
            CompletableFuture<?>[] futures = toInit.stream()
                    .map(type -> CompletableFuture.runAsync(() -> initPoolFor(type)))
                    .toArray(CompletableFuture[]::new);
            CompletableFuture.allOf(futures).join();
            log.info("[STARTUP] Datasources ready for: {}", toInit);
        } else {
            PipelineConfigSource single = pipelineConfigService.getEffectiveSourceConfig();
            if (single != null && types.stream().anyMatch(t -> t.equalsIgnoreCase(single.getType()))) {
                log.info("[STARTUP] Creating datasource for single source: {}", single.getType());
                initPoolFor(single.getType());
            }
        }
    }

    private void initPoolFor(String sourceKey) {
        try {
            PipelineConfigSource src = pipelineConfigService.getEffectiveSourceConfig(sourceKey);
            if (src != null) {
                // Config is reloaded from file on each getEffectiveSourceConfig call
                HikariDataSource.INSTANCE.getOrInit(src.toDbConfigSnapshot());
                log.debug("[STARTUP] Pool initialized for source key: {}", sourceKey);
            }
        } catch (Exception e) {
            PoolInitFailureRecorder.setLastFailure(sourceKey, e.getMessage());
            log.warn("[STARTUP] Failed to init pool for source '{}': {}", sourceKey, e.getMessage());
        }
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
}
