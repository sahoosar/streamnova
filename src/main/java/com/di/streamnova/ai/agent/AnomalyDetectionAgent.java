package com.di.streamnova.ai.agent;

import com.di.streamnova.ai.config.AiProperties;
import com.di.streamnova.ai.dto.AnomalyReport;
import com.di.streamnova.agent.metrics.MetricsLearningService;
import com.di.streamnova.agent.metrics.ThroughputProfile;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * AI agent that detects anomalies in pipeline execution metrics using Gemini.
 *
 * <p>After each pipeline run, the {@link MetricsLearningService} records throughput
 * profiles. This agent queries those profiles and asks Gemini to identify runs
 * that deviate significantly from historical norms.
 *
 * <p>Patterns detected:
 * <ul>
 *   <li>Throughput drops (possible DB bottleneck or network issue)</li>
 *   <li>Cost spikes (machine type or worker count unexpectedly high)</li>
 *   <li>Duration outliers (slow runs that should trigger investigation)</li>
 *   <li>Shard imbalance (some shards processing much more than others)</li>
 * </ul>
 */
@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(name = "streamnova.ai.enabled", havingValue = "true", matchIfMissing = true)
public class AnomalyDetectionAgent {

    @Qualifier("statelessChatClient")
    private final ChatClient            chatClient;
    private final AiProperties          aiProperties;
    private final MetricsLearningService metricsLearningService;

    @Value("classpath:prompts/anomaly-detection-system.st")
    private Resource systemPromptResource;

    /**
     * Analyses recent execution metrics for a machine type and flags anomalies.
     *
     * @param machineType  e.g. "n2d-standard-4"
     * @param lookbackRuns number of recent runs to analyse (default: 20)
     * @return structured anomaly report
     */
    public AnomalyReport analyseMetrics(String machineType, int lookbackRuns) {
        log.info("[ANOMALY] Analysing last {} runs for machineType={}", lookbackRuns, machineType);
        long start = System.currentTimeMillis();

        try {
            List<ThroughputProfile> history =
                    metricsLearningService.getThroughputHistory(machineType, lookbackRuns);

            if (history == null || history.isEmpty()) {
                return AnomalyReport.builder()
                        .machineType(machineType)
                        .success(true)
                        .summary("No historical runs found for " + machineType + ". Nothing to analyse.")
                        .anomalyDetected(false)
                        .build();
            }

            String metricsText = formatMetricsForPrompt(history);

            String prompt = String.format("""
                    Analyse the following Apache Beam / Dataflow pipeline execution metrics
                    for machine type '%s'.
                    Identify any anomalies, patterns, or concerning trends.
                    Flag: throughput drops, cost spikes, duration outliers.
                    Suggest root causes and corrective actions.

                    Metrics (last %d runs):
                    %s
                    """, machineType, history.size(), metricsText);

            String aiAnalysis = chatClient.prompt()
                    .system(systemPromptResource)
                    .user(prompt)
                    .call()
                    .content();

            boolean anomalyDetected = aiAnalysis.toLowerCase().contains("anomaly")
                    || aiAnalysis.toLowerCase().contains("spike")
                    || aiAnalysis.toLowerCase().contains("drop")
                    || aiAnalysis.toLowerCase().contains("outlier");

            long durationMs = System.currentTimeMillis() - start;

            return AnomalyReport.builder()
                    .machineType(machineType)
                    .runsAnalysed(history.size())
                    .summary(aiAnalysis)
                    .anomalyDetected(anomalyDetected)
                    .durationMs(durationMs)
                    .success(true)
                    .build();

        } catch (Exception ex) {
            log.error("[ANOMALY] Analysis failed for {}: {}", machineType, ex.getMessage());
            return AnomalyReport.builder()
                    .machineType(machineType)
                    .success(false)
                    .errorMessage(ex.getMessage())
                    .anomalyDetected(false)
                    .build();
        }
    }

    // ------------------------------------------------------------------ //

    private String formatMetricsForPrompt(List<ThroughputProfile> history) {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("%-5s %-12s %-12s %-10s%n", "#", "throughput", "duration(s)", "cost($)"));
        int i = 1;
        for (ThroughputProfile p : history) {
            sb.append(String.format("%-5d %-12.2f %-12.0f %-10.4f%n",
                    i++,
                    p.getThroughputMbPerSec(),
                    p.getDurationSec(),
                    p.getCostUsd()));
        }
        return sb.toString();
    }
}
