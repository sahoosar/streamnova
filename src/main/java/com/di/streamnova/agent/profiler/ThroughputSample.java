package com.di.streamnova.agent.profiler;

import lombok.Builder;
import lombok.Value;

import java.time.Instant;

/**
 * Result of a warm-up read: bytes read, duration, and derived throughput.
 * Used to discover source (Postgres) read throughput for Estimator and Recommender.
 */
@Value
@Builder
public class ThroughputSample {
    /** Source type (e.g. postgres). */
    String sourceType;
    /** Schema.table that was read. */
    String schemaName;
    String tableName;
    /** Total bytes read in the warm-up. */
    long bytesRead;
    /** Duration of the warm-up read in milliseconds. */
    long durationMs;
    /** Rows read during warm-up. */
    int rowsRead;
    /** Throughput in MB/s (bytesRead / (durationMs/1000) / (1024*1024)). */
    double throughputMbPerSec;
    /** When the sample was taken. */
    Instant sampledAt;

    public static ThroughputSample of(long bytesRead, long durationMs, int rowsRead,
                                      String sourceType, String schemaName, String tableName) {
        double durationSec = durationMs / 1000.0;
        double mbPerSec = durationSec > 0 ? (bytesRead / (1024.0 * 1024.0)) / durationSec : 0;
        return ThroughputSample.builder()
                .sourceType(sourceType != null ? sourceType : "postgres")
                .schemaName(schemaName != null ? schemaName : "public")
                .tableName(tableName)
                .bytesRead(bytesRead)
                .durationMs(durationMs)
                .rowsRead(rowsRead)
                .throughputMbPerSec(mbPerSec)
                .sampledAt(Instant.now())
                .build();
    }
}
