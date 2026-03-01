package com.di.streamnova.ai.tool;

import com.di.streamnova.agent.estimator.EstimatedCandidate;
import com.di.streamnova.agent.estimator.EstimatorService;
import com.di.streamnova.agent.recommender.RecommendationTriple;
import com.di.streamnova.agent.recommender.RecommenderService;
import com.di.streamnova.agent.recommender.UserMode;
import com.di.streamnova.agent.shardplanner.ShardPlanner;
import com.di.streamnova.config.TableConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.ai.tool.annotation.ToolParam;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Spring AI Tool — wraps {@link RecommenderService} so that Gemini can call it
 * autonomously when the user asks "what machine type should I use?" or
 * "give me the cheapest pipeline configuration".
 *
 * <p>Annotate with {@code @Tool} — Spring AI auto-registers these as function
 * definitions in the chat request so Gemini decides when to invoke them.
 */
@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(name = "streamnova.ai.enabled", havingValue = "true", matchIfMissing = true)
public class RecommendationTool {

    private final RecommenderService recommenderService;
    private final EstimatorService   estimatorService;
    private final ShardPlanner       shardPlanner;

    /**
     * Returns cheapest / fastest / balanced pipeline candidate recommendations
     * for the given table size and row count.
     */
    @Tool(description = """
            Recommends the best Dataflow pipeline execution candidates (machine type, worker count,
            shard count) for a given table size and row count.
            Returns three options: cheapest (lowest cost), fastest (lowest duration), and balanced.
            Use this when the user asks which machine type to use or how to configure the pipeline.
            """)
    public String recommendPipelineCandidates(
            @ToolParam(description = "Estimated table size in MB, e.g. 5000")
            double tableSizeMb,
            @ToolParam(description = "Estimated row count, e.g. 10000000")
            long rowCount,
            @ToolParam(description = "Source database type: postgres | oracle | mysql | gcs | bigquery")
            String sourceType,
            @ToolParam(description = "Target type: gcs | bigquery")
            String targetType
    ) {
        log.info("[TOOL:recommend] tableSizeMb={} rowCount={} source={} target={}",
                tableSizeMb, rowCount, sourceType, targetType);
        try {
            TableConfig tableConfig = TableConfig.builder()
                    .estimatedRows(rowCount)
                    .estimatedSizeMb(tableSizeMb)
                    .build();

            List<EstimatedCandidate> candidates =
                    estimatorService.estimateCandidates(tableConfig, sourceType, targetType);

            RecommendationTriple triple =
                    recommenderService.recommendCheapestFastestBalanced(candidates, null);

            return formatTriple(triple);
        } catch (Exception ex) {
            log.error("[TOOL:recommend] failed: {}", ex.getMessage());
            return "Could not generate recommendations: " + ex.getMessage();
        }
    }

    /**
     * Calculates optimal shard count for a given table profile.
     */
    @Tool(description = """
            Calculates the optimal number of shards for parallel data extraction
            given the row count, table size, and machine type.
            """)
    public String calculateOptimalShards(
            @ToolParam(description = "Estimated row count")           long rowCount,
            @ToolParam(description = "Estimated table size in MB")    double tableSizeMb,
            @ToolParam(description = "Machine type, e.g. n2d-standard-4") String machineType,
            @ToolParam(description = "Number of Dataflow workers")    int workerCount
    ) {
        log.info("[TOOL:shards] rows={} sizeMb={} machine={} workers={}",
                rowCount, tableSizeMb, machineType, workerCount);
        try {
            int shards = shardPlanner.calculateShards(rowCount, tableSizeMb, machineType, workerCount);
            return String.format(
                    "Recommended shard count: %d  (rows=%d, size=%.0f MB, machine=%s, workers=%d)",
                    shards, rowCount, tableSizeMb, machineType, workerCount);
        } catch (Exception ex) {
            log.error("[TOOL:shards] failed: {}", ex.getMessage());
            return "Could not calculate shards: " + ex.getMessage();
        }
    }

    // ------------------------------------------------------------------ //

    private String formatTriple(RecommendationTriple t) {
        StringBuilder sb = new StringBuilder("Pipeline Recommendations:\n");
        appendCandidate(sb, "CHEAPEST",  t.getCheapest());
        appendCandidate(sb, "FASTEST",   t.getFastest());
        appendCandidate(sb, "BALANCED",  t.getBalanced());
        if (t.getGuardrailViolations() != null && !t.getGuardrailViolations().isEmpty()) {
            sb.append("Guardrail violations: ").append(t.getGuardrailViolations());
        }
        return sb.toString();
    }

    private void appendCandidate(StringBuilder sb, String label, EstimatedCandidate c) {
        if (c == null || c.getCandidate() == null) {
            sb.append("  ").append(label).append(": not available\n");
            return;
        }
        sb.append(String.format("  %s: machine=%s workers=%d shards=%d  est.duration=%.0fs est.cost=$%.4f%n",
                label,
                c.getCandidate().getMachineType(),
                c.getCandidate().getWorkerCount(),
                c.getCandidate().getShardCount(),
                c.getEstimatedDurationSec(),
                c.getEstimatedCostUsd()));
    }
}
