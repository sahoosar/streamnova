package com.di.streamnova.config;

import com.di.streamnova.handler.SourceHandlerRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.Optional;

/**
 * Pipeline is not fixed to a particular handler. At startup, detects which handlers are required from config
 * (source, intermediate, target). At execution, the pipeline recognises the source type from config and runs
 * the matching handler: OracleHandler, PostgresHandler, GCSHandler, or any other registered handler.
 * <ul>
 *   <li>Source: config defaultSource / connection type (postgres, oracle, gcs, …) → registry returns the matching handler.</li>
 *   <li>Intermediate: config intermediate (e.g. gcs).</li>
 *   <li>Target: config target.type (e.g. bigquery).</li>
 * </ul>
 * To use a different source handler, change pipeline config (e.g. defaultSource: oracle or postgres or gcs); no code change.
 */
@Component
@Order(2)
@RequiredArgsConstructor
@Slf4j
public class PipelineHandlerDetectionStartup implements ApplicationRunner {

    private final PipelineConfigService pipelineConfigService;
    private final SourceHandlerRegistry sourceHandlerRegistry;

    @Override
    public void run(ApplicationArguments args) {
        LoadConfig loadConfig = pipelineConfigService.getEffectiveLoadConfig();
        if (loadConfig == null || loadConfig.getSource() == null) {
            return;
        }

        PipelineHandlerRequirements requirements = pipelineConfigService.getPipelineHandlerRequirements(loadConfig);
        if (requirements == null) {
            return;
        }

        String sourceType = requirements.getSourceHandlerType();
        Optional<String> sourceHandlerClass = sourceHandlerRegistry.getHandlerClassName(sourceType);
        String sourceHandlerDisplay = sourceHandlerClass.map(c -> sourceType + " → " + c).orElse(sourceType + " (no handler registered)");

        String intermediateDisplay = requirements.hasIntermediate() ? requirements.getIntermediateHandlerType() : "none";
        String targetDisplay = requirements.getTargetHandlerType() != null && !requirements.getTargetHandlerType().isBlank()
                ? requirements.getTargetHandlerType() : "none";

        log.info("[HANDLER-DETECT] Pipeline is not fixed to one handler; config determines which runs. Detected: source={}, intermediate={}, target={}",
                sourceHandlerDisplay, intermediateDisplay, targetDisplay);
        log.info("[HANDLER-DETECT] Execution will use the handler for this source type (change defaultSource/connection type in config to switch to OracleHandler, PostgresHandler, GCSHandler, etc.).");
    }
}
