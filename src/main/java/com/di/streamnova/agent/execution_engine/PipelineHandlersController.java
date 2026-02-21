package com.di.streamnova.agent.execution_engine;

import com.di.streamnova.config.LoadConfig;
import com.di.streamnova.config.PipelineConfigService;
import com.di.streamnova.config.PipelineHandlerRequirements;
import com.di.streamnova.handler.SourceHandlerRegistry;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * API to discover available source handlers. Returns availableSourceHandlers, message, handlers, configured, and currentPipelineHandlers.
 */
@RestController
@RequestMapping("/api/agent/pipeline-handlers")
@RequiredArgsConstructor
public class PipelineHandlersController {

    private final SourceHandlerRegistry sourceHandlerRegistry;
    private final PipelineConfigService pipelineConfigService;

    /**
     * Returns availableSourceHandlers, message, handlers (type → descriptor), configured (source/intermediate/target keys), and currentPipelineHandlers.
     */
    @GetMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<PipelineHandlersResponse> getPipelineHandlers() {
        Set<String> sourceTypes = sourceHandlerRegistry.getRegisteredTypes();
        List<String> availableSourceHandlers = sourceTypes.stream().sorted().collect(Collectors.toList());

        Set<String> configuredSourceKeys = pipelineConfigService.getConfiguredSourceKeys();
        Set<String> configuredIntermediateKeys = pipelineConfigService.getConfiguredIntermediateKeys();
        Set<String> configuredDestinationKeys = pipelineConfigService.getConfiguredDestinationKeys();

        Map<String, PipelineHandlersResponse.HandlerDescriptor> handlers = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (String type : sourceTypes) {
            String className = sourceHandlerRegistry.getHandlerClassName(type).orElse("");
            List<String> roles = new ArrayList<>();
            roles.add("source");
            if (configuredIntermediateKeys.contains(type)) roles.add("intermediate");
            if (configuredDestinationKeys.contains(type)) roles.add("target");
            handlers.put(type, PipelineHandlersResponse.HandlerDescriptor.builder()
                    .className(className)
                    .roles(roles)
                    .build());
        }
        for (String type : configuredIntermediateKeys) {
            if (!handlers.containsKey(type)) {
                handlers.put(type, PipelineHandlersResponse.HandlerDescriptor.builder()
                        .className("")
                        .roles(List.of("intermediate"))
                        .build());
            } else {
                if (!handlers.get(type).getRoles().contains("intermediate")) {
                    List<String> roles = new ArrayList<>(handlers.get(type).getRoles());
                    roles.add("intermediate");
                    handlers.put(type, PipelineHandlersResponse.HandlerDescriptor.builder()
                            .className(handlers.get(type).getClassName())
                            .roles(roles)
                            .build());
                }
            }
        }
        for (String type : configuredDestinationKeys) {
            if (!handlers.containsKey(type)) {
                handlers.put(type, PipelineHandlersResponse.HandlerDescriptor.builder()
                        .className("")
                        .roles(List.of("target"))
                        .build());
            } else {
                if (!handlers.get(type).getRoles().contains("target")) {
                    List<String> roles = new ArrayList<>(handlers.get(type).getRoles());
                    roles.add("target");
                    handlers.put(type, PipelineHandlersResponse.HandlerDescriptor.builder()
                            .className(handlers.get(type).getClassName())
                            .roles(roles)
                            .build());
                }
            }
        }

        Map<String, List<String>> configured = new LinkedHashMap<>();
        configured.put("source", new ArrayList<>(configuredSourceKeys));
        configured.put("intermediate", new ArrayList<>(configuredIntermediateKeys));
        configured.put("target", new ArrayList<>(configuredDestinationKeys));

        Map<String, String> current = new LinkedHashMap<>();
        LoadConfig loadConfig = pipelineConfigService.getEffectiveLoadConfig();
        if (loadConfig != null) {
            PipelineHandlerRequirements req = pipelineConfigService.getPipelineHandlerRequirements(loadConfig);
            if (req != null) {
                current.put("source", req.getSourceHandlerType());
                current.put("intermediate", req.hasIntermediate() ? req.getIntermediateHandlerType() : "none");
                current.put("target", req.getTargetHandlerType() != null ? req.getTargetHandlerType() : "none");
            }
        }

        String message = "Use these handler keys for source (and optionally intermediate, target) in GET /api/agent/pipeline-listener/actions-mapping and POST /api/agent/execute. "
                + "Omit 'intermediate' for 2-stage (source → target); include 'intermediate' for 3-stage (source → intermediate → target).";

        return ResponseEntity.ok(PipelineHandlersResponse.builder()
                .availableSourceHandlers(availableSourceHandlers)
                .message(message)
                .handlers(handlers)
                .configured(configured)
                .currentPipelineHandlers(current)
                .build());
    }
}
