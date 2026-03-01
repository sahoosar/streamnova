package com.di.streamnova.ai.controller;

import com.di.streamnova.ai.dto.AiChatRequest;
import com.di.streamnova.ai.dto.AiChatResponse;
import com.di.streamnova.ai.dto.AnomalyReport;
import com.di.streamnova.ai.dto.DataQualityReport;
import com.di.streamnova.ai.service.AiPipelineService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.MediaType;
import org.springframework.web.servlet.mvc.method.annotation.ResponseBodyEmitter;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * REST API for all AI / Gemini-powered features in StreamNova.
 *
 * <p>Base path: {@code /api/ai}
 *
 * <pre>
 * POST /api/ai/chat              → Conversational pipeline advisor (full response)
 * POST /api/ai/chat/stream       → Streaming token-by-token response (SSE)
 * POST /api/ai/data-quality      → AI-powered data quality assessment
 * GET  /api/ai/anomalies         → Anomaly detection for a machine type
 * GET  /api/ai/status            → AI configuration status
 * </pre>
 *
 * <p>All endpoints are disabled gracefully when {@code streamnova.ai.enabled=false}.
 */
@Slf4j
@RestController
@RequestMapping("/api/ai")
@RequiredArgsConstructor
@ConditionalOnProperty(name = "streamnova.ai.enabled", havingValue = "true", matchIfMissing = true)
public class AiAgentController {

    private final AiPipelineService aiPipelineService;

    // ------------------------------------------------------------------ //
    // Chat                                                                //
    // ------------------------------------------------------------------ //

    /**
     * Conversational pipeline advisor. Gemini will call tools autonomously.
     *
     * <p>Example request:
     * <pre>{@code
     * POST /api/ai/chat
     * {
     *   "message": "Profile my postgres orders table and recommend the best machine type",
     *   "conversationId": "optional-session-id"
     * }
     * }</pre>
     */
    @PostMapping(
            path  = "/chat",
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<AiChatResponse> chat(@Valid @RequestBody AiChatRequest request) {
        log.info("[AI-CTRL] POST /chat conversationId={}", request.getConversationId());
        AiChatResponse response = aiPipelineService.chat(request);
        return response.isSuccess()
                ? ResponseEntity.ok(response)
                : ResponseEntity.status(500).body(response);
    }

    /**
     * Streaming chat — tokens delivered as Server-Sent Events (SSE).
     * Use {@code Accept: text/event-stream} on the client.
     */
    @PostMapping(
            path  = "/chat/stream",
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public ResponseBodyEmitter streamChat(@Valid @RequestBody AiChatRequest request) {
        log.info("[AI-CTRL] POST /chat/stream conversationId={}", request.getConversationId());
        return aiPipelineService.streamChat(request);
    }

    // ------------------------------------------------------------------ //
    // Data Quality                                                        //
    // ------------------------------------------------------------------ //

    /**
     * AI-powered data quality assessment for a source table.
     *
     * <p>Example:
     * <pre>{@code
     * POST /api/ai/data-quality?sourceType=postgres&schema=public&table=orders&pkColumn=id
     * }</pre>
     */
    @PostMapping(path = "/data-quality", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<DataQualityReport> dataQuality(
            @RequestParam @NotBlank String sourceType,
            @RequestParam @NotBlank String schema,
            @RequestParam @NotBlank String table,
            @RequestParam(defaultValue = "id") String pkColumn
    ) {
        log.info("[AI-CTRL] POST /data-quality source={} schema={} table={}", sourceType, schema, table);
        DataQualityReport report = aiPipelineService.assessDataQuality(sourceType, schema, table, pkColumn);
        return report.isSuccess()
                ? ResponseEntity.ok(report)
                : ResponseEntity.status(500).body(report);
    }

    // ------------------------------------------------------------------ //
    // Anomaly Detection                                                   //
    // ------------------------------------------------------------------ //

    /**
     * Analyse recent execution metrics for anomalies.
     *
     * <p>Example:
     * <pre>{@code
     * GET /api/ai/anomalies?machineType=n2d-standard-4&lookback=20
     * }</pre>
     */
    @GetMapping(path = "/anomalies", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<AnomalyReport> anomalies(
            @RequestParam(defaultValue = "n2d-standard-4") String machineType,
            @RequestParam(defaultValue = "20") @Min(1) int lookback
    ) {
        log.info("[AI-CTRL] GET /anomalies machineType={} lookback={}", machineType, lookback);
        AnomalyReport report = aiPipelineService.detectAnomalies(machineType, lookback);
        return report.isSuccess()
                ? ResponseEntity.ok(report)
                : ResponseEntity.status(500).body(report);
    }

    // ------------------------------------------------------------------ //
    // Status                                                              //
    // ------------------------------------------------------------------ //

    /**
     * Returns the current AI configuration — safe to expose, no secrets.
     */
    @GetMapping(path = "/status", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Map<String, Object>> status() {
        return ResponseEntity.ok(aiPipelineService.getStatus());
    }
}
