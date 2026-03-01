package com.di.streamnova.agent.recommender;

import com.di.streamnova.agent.capacity.CapacityMessageService;
import com.di.streamnova.agent.capacity.ResourceLimitResponse;
import com.di.streamnova.agent.estimator.LoadPattern;
import com.di.streamnova.agent.recommender.RecommendationResult;
import com.di.streamnova.agent.trigger.AgentTriggerService;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * API for low-cost vs speed: run Profiler → Candidates → Estimator → Recommender by mode.
 * GET /streamnova/api/agent/recommend?mode=COST_OPTIMAL | FAST_LOAD | BALANCED
 *
 * <p>Guardrail logic is fully extracted to {@link GuardrailService} / {@link com.di.streamnova.guardrail.GuardrailProperties}.
 * No duplicate @Value guardrail fields remain here — all limits are YAML-driven.
 */
@RestController
@RequestMapping("/api/agent/recommend")
@RequiredArgsConstructor
public class RecommendationController {

    private final AgentTriggerService  agentTriggerService;
    private final CapacityMessageService capacityMessageService;

    @Value("${streamnova.recommend.max-concurrent-requests}")
    private int maxConcurrentRequests;
    @Value("${streamnova.recommend.concurrent-request-acquire-timeout-sec}")
    private int concurrentRequestAcquireTimeoutSec;

    private Semaphore recommendSemaphore;

    @PostConstruct
    void initConcurrencyLimit() {
        recommendSemaphore = (maxConcurrentRequests > 0)
                ? new Semaphore(maxConcurrentRequests) : null;
    }

    /**
     * Runs full pipeline: profile → candidates → estimate → recommend by mode.
     *
     * @param maxCostUsd            per-request override (overrides YAML when provided)
     * @param maxDurationSec        per-request override (overrides YAML when provided)
     * @param minThroughputMbPerSec per-request override (overrides YAML when provided)
     * @param allowedMachineTypes   per-request override (overrides YAML when provided)
     */
    @GetMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> recommend(
            @RequestParam(defaultValue = "BALANCED") UserMode mode,
            @RequestParam(required = false) String source,
            @RequestParam(required = false, defaultValue = "true") boolean warmUp,
            @RequestParam(required = false) LoadPattern loadPattern,
            @RequestParam(required = false) Double maxCostUsd,
            @RequestParam(required = false) Double maxDurationSec,
            @RequestParam(required = false) Double minThroughputMbPerSec,
            @RequestParam(required = false) List<String> allowedMachineTypes,
            @RequestParam(required = false) Integer databasePoolMaxSize) {

        if (recommendSemaphore != null) {
            boolean acquired;
            try {
                acquired = recommendSemaphore.tryAcquire(
                        Math.max(1, concurrentRequestAcquireTimeoutSec), TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return ResponseEntity.status(429).body(capacityMessageService.buildResourceLimitResponse());
            }
            if (!acquired) {
                ResourceLimitResponse body = capacityMessageService.buildResourceLimitResponse();
                return ResponseEntity.status(429)
                        .header("Retry-After", body.getRetryAfterSeconds() != null
                                ? String.valueOf(body.getRetryAfterSeconds()) : null)
                        .body(body);
            }
        }
        try {
            Optional<RecommendationResult> result = agentTriggerService.recommend(
                    mode, source, warmUp, loadPattern,
                    maxCostUsd, maxDurationSec, minThroughputMbPerSec,
                    allowedMachineTypes, databasePoolMaxSize, null);
            return result.map(ResponseEntity::ok).orElse(ResponseEntity.noContent().build());
        } finally {
            if (recommendSemaphore != null) recommendSemaphore.release();
        }
    }
}
