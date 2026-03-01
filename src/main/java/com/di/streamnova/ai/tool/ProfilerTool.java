package com.di.streamnova.ai.tool;

import com.di.streamnova.agent.profiler.ProfilerService;
import com.di.streamnova.agent.profiler.TableProfile;
import com.di.streamnova.config.TableConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.ai.tool.annotation.ToolParam;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Spring AI Tool — wraps {@link ProfilerService} so that Gemini can profile
 * a source table before recommending pipeline parameters.
 *
 * <p>Typical flow: user says "profile my postgres table orders" →
 * Gemini calls {@link #profileTable} → then calls {@link RecommendationTool#recommendPipelineCandidates}.
 */
@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(name = "streamnova.ai.enabled", havingValue = "true", matchIfMissing = true)
public class ProfilerTool {

    private final ProfilerService profilerService;

    /**
     * Profiles a database table to measure throughput, row count, and data size.
     */
    @Tool(description = """
            Profiles a source database table by sampling rows and measuring throughput.
            Returns estimated row count, data size (MB), throughput (MB/s), and data types.
            Use this before recommending pipeline configurations.
            Source type: postgres | oracle | mysql | gcs | bigquery.
            """)
    public String profileTable(
            @ToolParam(description = "Source type: postgres | oracle | mysql | gcs | bigquery")
            String sourceType,
            @ToolParam(description = "Schema name, e.g. public")
            String schema,
            @ToolParam(description = "Table name, e.g. orders")
            String tableName
    ) {
        log.info("[TOOL:profiler] source={} schema={} table={}", sourceType, schema, tableName);
        try {
            TableProfile profile = profilerService.profileTable(sourceType, schema, tableName);
            if (profile == null) {
                return String.format("Could not profile table %s.%s: no profile result.", schema, tableName);
            }
            double sizeMb = profile.getEstimatedTotalBytes() / (1024.0 * 1024.0);
            return String.format("""
                    Table Profile: %s.%s (source=%s)
                      Estimated rows:       %,d
                      Estimated size:       %.1f MB
                      Warm-up throughput:   N/A
                      Columns:              N/A
                      Has partitions:       N/A
                    """,
                    schema, tableName, sourceType,
                    profile.getRowCountEstimate(),
                    sizeMb);
        } catch (Exception ex) {
            log.warn("[TOOL:profiler] Could not profile {}.{}: {}", schema, tableName, ex.getMessage());
            return String.format("Could not profile table %s.%s: %s", schema, tableName, ex.getMessage());
        }
    }

    /**
     * Returns the latest cached profile for a table (fast; no DB roundtrip).
     */
    @Tool(description = """
            Returns the last known profile for a table from the in-memory cache.
            Returns null if no profile exists yet — call profileTable first in that case.
            """)
    public String getCachedProfile(
            @ToolParam(description = "Source type: postgres | oracle | mysql")
            String sourceType,
            @ToolParam(description = "Table name")
            String tableName
    ) {
        try {
            TableProfile cached = profilerService.getCachedProfile(sourceType, tableName);
            if (cached == null) {
                return "No cached profile found for " + tableName + ". Call profileTable first.";
            }
            double sizeMb = cached.getEstimatedTotalBytes() / (1024.0 * 1024.0);
            return String.format("Cached profile for %s (%s): rows=%,d size=%.1f MB",
                    tableName, sourceType,
                    cached.getRowCountEstimate(),
                    sizeMb);
        } catch (Exception ex) {
            return "Could not retrieve cached profile: " + ex.getMessage();
        }
    }
}
