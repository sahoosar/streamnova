package com.di.streamnova.agent.profiler;

import com.di.streamnova.config.PipelineConfigService;
import com.di.streamnova.handler.jdbc.postgres.PostgresStatisticsEstimator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.UUID;

/**
 * Orchestrates the Profiler segment: table profiling (row count, avg row size) and optional
 * Postgres warm-up read for throughput discovery. Results are used by Candidate Generator and
 * Estimator, and can be stored for Metrics & Learning Store.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ProfilerService {

    private final PipelineConfigService pipelineConfigService;
    private final ProfileStore profileStore;

    @Value("${streamnova.profiler.warm-up-rows}")
    private int warmUpRows;
    @Value("${streamnova.profiler.warm-up-fetch-size}")
    private int warmUpFetchSize;

    /**
     * Runs full profiling for the default source: table stats + warm-up read.
     *
     * @return profile result with table profile and throughput sample (or null if config unavailable)
     */
    public ProfileResult profile() {
        return profile(null, true);
    }

    /**
     * Runs profiling for the given source key.
     *
     * @param sourceKey     source key (e.g. "postgres") or null for default
     * @param runWarmUp     whether to run a warm-up read for throughput discovery
     * @return profile result, or null if config unavailable
     */
    public ProfileResult profile(String sourceKey, boolean runWarmUp) {
        var config = pipelineConfigService.getEffectiveSourceConfig(sourceKey);
        if (config == null) {
            log.warn("[PROFILER] No pipeline source config; skip profiling");
            return null;
        }
        return profileWithConfig(config, runWarmUp);
    }

    /**
     * Runs profiling using the given pipeline source config (e.g. from API).
     */
    public ProfileResult profileWithConfig(com.di.streamnova.config.PipelineConfigSource config, boolean runWarmUp) {
        String runId = UUID.randomUUID().toString();
        log.info("[PROFILER] Starting profile runId={} table={}", runId, config.getTable());

        TableProfile tableProfile;
        try {
            com.di.streamnova.handler.jdbc.postgres.PostgresStatisticsEstimator.TableStatistics stats =
                    estimateStatistics(config);
            tableProfile = TableProfile.from(
                    stats.rowCount(),
                    stats.avgRowSizeBytes(),
                    "postgres",
                    schemaFrom(config.getTable()),
                    tableFrom(config.getTable()));
        } catch (Exception e) {
            log.error("[PROFILER] Statistics estimation failed: {}", e.getMessage(), e);
            return ProfileResult.builder()
                    .runId(runId)
                    .tableProfile(null)
                    .throughputSample(null)
                    .completedAt(java.time.Instant.now())
                    .errorMessage("Statistics failed: " + e.getMessage())
                    .build();
        }

        ThroughputSample throughputSample = null;
        if (runWarmUp && tableProfile.getRowCountEstimate() > 0) {
            throughputSample = WarmUpReader.runWarmUp(config, tableProfile.getAvgRowSizeBytes(), warmUpRows, warmUpFetchSize);
        }

        ProfileResult result = ProfileResult.builder()
                .runId(runId)
                .tableProfile(tableProfile)
                .throughputSample(throughputSample)
                .completedAt(java.time.Instant.now())
                .errorMessage(null)
                .build();

        log.info("[PROFILER] Completed runId={} rowCount={} avgRowBytes={} throughputMbPerSec={}",
                runId, tableProfile.getRowCountEstimate(), tableProfile.getAvgRowSizeBytes(),
                throughputSample != null ? String.format("%.2f", throughputSample.getThroughputMbPerSec()) : "N/A");

        profileStore.save(result);
        return result;
    }

    private PostgresStatisticsEstimator.TableStatistics estimateStatistics(com.di.streamnova.config.PipelineConfigSource config) {
        return PostgresStatisticsEstimator.estimateStatistics(config);
    }

    private static String schemaFrom(String fullTable) {
        if (fullTable == null || fullTable.isBlank()) return "public";
        String t = fullTable.trim();
        int dot = t.indexOf('.');
        return dot > 0 ? t.substring(0, dot).trim() : "public";
    }

    private static String tableFrom(String fullTable) {
        if (fullTable == null || fullTable.isBlank()) return fullTable;
        String t = fullTable.trim();
        int dot = t.indexOf('.');
        return dot > 0 ? t.substring(dot + 1).trim() : t;
    }
}
