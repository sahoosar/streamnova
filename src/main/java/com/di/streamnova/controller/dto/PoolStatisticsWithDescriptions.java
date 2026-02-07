package com.di.streamnova.controller.dto;

import com.di.streamnova.util.PoolStatsSnapshot;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Pool statistics with summary, optional error, per-source errors, and per-pool metrics.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PoolStatisticsWithDescriptions {

    private String summary;
    private String error;
    private Map<String, String> sourceErrors;
    private List<PoolStatsEntryWithDescription> pools;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class PoolStatsEntryWithDescription {
        private String sourceKey;
        private String poolName;
        private String description;
        private int maxPoolSize;
        private int minIdle;
        private int activeConnections;
        private int idleConnections;
        private int totalConnections;
        private int threadsAwaitingConnection;
    }

    public static PoolStatisticsWithDescriptions from(List<PoolStatsSnapshot> snapshots, List<String> sourceKeysInOrder, String reasonWhenNoPools, Map<String, String> sourceErrors) {
        boolean noPools = snapshots == null || snapshots.isEmpty();
        String summary;
        String error;
        if (noPools && reasonWhenNoPools != null && !reasonWhenNoPools.isBlank()) {
            summary = reasonWhenNoPools;
            error = reasonWhenNoPools;
        } else if (noPools) {
            summary = "No connection pools created.";
            error = "No datasource created. Check streamnova.pipeline.config-file and startup logs.";
        } else {
            summary = "Live HikariCP connection pool metrics. One entry per database connection pool.";
            error = null;
        }
        List<PoolStatsEntryWithDescription> pools = snapshots == null ? List.of() : snapshots.stream()
                .map(s -> toEntry(s, sourceKeysInOrder, snapshots.indexOf(s)))
                .collect(Collectors.toList());
        return PoolStatisticsWithDescriptions.builder()
                .summary(summary)
                .error(error)
                .sourceErrors(sourceErrors != null && !sourceErrors.isEmpty() ? sourceErrors : null)
                .pools(pools)
                .build();
    }

    private static PoolStatsEntryWithDescription toEntry(PoolStatsSnapshot s, List<String> sourceKeysInOrder, int index) {
        String sourceKey = (sourceKeysInOrder != null && index >= 0 && index < sourceKeysInOrder.size())
                ? sourceKeysInOrder.get(index) : null;
        String desc = describePool(s, sourceKeysInOrder, index);
        return PoolStatsEntryWithDescription.builder()
                .sourceKey(sourceKey)
                .poolName(s.poolName())
                .description(desc)
                .maxPoolSize(s.maxPoolSize())
                .minIdle(s.minIdle())
                .activeConnections(s.activeConnections())
                .idleConnections(s.idleConnections())
                .totalConnections(s.totalConnections())
                .threadsAwaitingConnection(s.threadsAwaitingConnection())
                .build();
    }

    private static String describePool(PoolStatsSnapshot s, List<String> sourceKeysInOrder, int index) {
        String name = s.poolName();
        if (name == null) return "Connection pool";
        String suffix = name.replaceFirst("^HikariPool-\\d+-", "");
        if (sourceKeysInOrder != null && index >= 0 && index < sourceKeysInOrder.size()) {
            String key = sourceKeysInOrder.get(index);
            return key + " pool (" + suffix.replace('_', ' ') + ")";
        }
        return "Pool: " + suffix.replace('_', ' ');
    }
}
