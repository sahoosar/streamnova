package com.di.streamnova.agent.profiler;

import lombok.Builder;
import lombok.Value;

import java.time.Instant;

/**
 * Table-level profile: row count estimate, average row size, and optional complexity.
 * Produced by the Profiler segment for use by Candidate Generator and Estimator.
 */
@Value
@Builder
public class TableProfile {
    /** Source type (e.g. postgres, oracle). */
    String sourceType;
    /** Schema name (e.g. public). */
    String schemaName;
    /** Table name. */
    String tableName;
    /** Estimated row count (from pg_class or COUNT(*)). */
    long rowCountEstimate;
    /** Average row size in bytes (from pg_class or sampling). */
    int avgRowSizeBytes;
    /** Estimated total size in bytes (rowCount * avgRowSize). */
    long estimatedTotalBytes;
    /**
     * Optional complexity hint: SIMPLE, MODERATE, COMPLEX (e.g. wide table, many columns, LOBs).
     * Used later by Estimator for throughput caps.
     */
    String complexity;
    /** When this profile was taken. */
    Instant profiledAt;

    public static TableProfile from(long rowCount, int avgRowSizeBytes, String sourceType, String schemaName, String tableName) {
        long total = rowCount * Math.max(1, avgRowSizeBytes);
        String complexity = deriveComplexity(avgRowSizeBytes);
        return TableProfile.builder()
                .sourceType(sourceType != null ? sourceType : "postgres")
                .schemaName(schemaName != null ? schemaName : "public")
                .tableName(tableName)
                .rowCountEstimate(rowCount)
                .avgRowSizeBytes(avgRowSizeBytes)
                .estimatedTotalBytes(total)
                .complexity(complexity)
                .profiledAt(Instant.now())
                .build();
    }

    private static String deriveComplexity(int avgRowSizeBytes) {
        if (avgRowSizeBytes <= 500) return "SIMPLE";
        if (avgRowSizeBytes <= 2000) return "MODERATE";
        return "COMPLEX";
    }
}
