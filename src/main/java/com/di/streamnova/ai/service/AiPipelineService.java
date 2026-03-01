package com.di.streamnova.ai.service;

import com.di.streamnova.ai.agent.AnomalyDetectionAgent;
import com.di.streamnova.ai.agent.DataQualityAgent;
import com.di.streamnova.ai.agent.PipelineAdvisorAgent;
import com.di.streamnova.ai.config.AiProperties;
import com.di.streamnova.ai.dto.AiChatRequest;
import com.di.streamnova.ai.dto.AiChatResponse;
import com.di.streamnova.ai.dto.AnomalyReport;
import com.di.streamnova.ai.dto.DataQualityReport;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.servlet.mvc.method.annotation.ResponseBodyEmitter;
import org.springframework.stereotype.Service;

/**
 * Facade service that orchestrates all AI agents for StreamNova.
 *
 * <p>Controllers call this service — they never instantiate agents directly.
 * This keeps the controller layer thin and makes it easy to swap, mock,
 * or extend agents without touching HTTP-layer code.
 *
 * <h3>Extension pattern</h3>
 * <p>To add a new AI feature:
 * <ol>
 *   <li>Create a new {@code @Tool} class</li>
 *   <li>Inject it into the relevant agent (or create a new agent)</li>
 *   <li>Add a method here to expose it</li>
 *   <li>Add a route in {@code AiAgentController}</li>
 * </ol>
 */
@Slf4j
@Service
@RequiredArgsConstructor
@ConditionalOnProperty(name = "streamnova.ai.enabled", havingValue = "true", matchIfMissing = true)
public class AiPipelineService {

    private final PipelineAdvisorAgent    pipelineAdvisorAgent;
    private final DataQualityAgent        dataQualityAgent;
    private final AnomalyDetectionAgent   anomalyDetectionAgent;
    private final AiProperties            aiProperties;

    // ------------------------------------------------------------------ //
    // Pipeline Advisor                                                    //
    // ------------------------------------------------------------------ //

    /**
     * Conversational pipeline advisor — answers any question about the pipeline
     * using Gemini + tool calls.
     */
    public AiChatResponse chat(AiChatRequest request) {
        return pipelineAdvisorAgent.chat(request);
    }

    /**
     * Streaming version — for real-time token delivery to front-ends.
     */
    public ResponseBodyEmitter streamChat(AiChatRequest request) {
        return pipelineAdvisorAgent.streamChat(request);
    }

    // ------------------------------------------------------------------ //
    // Data Quality                                                        //
    // ------------------------------------------------------------------ //

    /**
     * Runs a full AI-powered data quality assessment for a source table.
     */
    public DataQualityReport assessDataQuality(String sourceType, String schema,
                                               String tableName, String pkColumn) {
        return dataQualityAgent.assess(sourceType, schema, tableName, pkColumn);
    }

    /**
     * Convenience wrapper: ask the advisor a DQ question in natural language.
     */
    public AiChatResponse askDataQualityQuestion(String question) {
        return pipelineAdvisorAgent.chat(
                AiChatRequest.builder().message(question).build());
    }

    // ------------------------------------------------------------------ //
    // Anomaly Detection                                                   //
    // ------------------------------------------------------------------ //

    /**
     * Analyses recent pipeline execution metrics and flags anomalies.
     */
    public AnomalyReport detectAnomalies(String machineType, int lookbackRuns) {
        int effectiveLookback = lookbackRuns > 0 ? lookbackRuns : 20;
        return anomalyDetectionAgent.analyseMetrics(machineType, effectiveLookback);
    }

    // ------------------------------------------------------------------ //
    // Status                                                              //
    // ------------------------------------------------------------------ //

    /**
     * Returns the current AI configuration status (safe — no secrets).
     */
    public java.util.Map<String, Object> getStatus() {
        return java.util.Map.of(
                "enabled",     aiProperties.isEnabled(),
                "model",       aiProperties.getChat().getModel(),
                "projectId",   aiProperties.getVertex().getProjectId(),
                "location",    aiProperties.getVertex().getLocation(),
                "maxIterations", aiProperties.getAgent().getMaxIterations(),
                "memoryEnabled", aiProperties.getAgent().isMemoryEnabled()
        );
    }
}
