package com.di.streamnova.agent.metrics;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * In-memory implementation of MetricsLearningStore. Suitable for single-node and testing.
 * When streamnova.metrics.persistence-enabled=true, JdbcMetricsLearningStore is used instead.
 */
@Component
@ConditionalOnProperty(name = "streamnova.metrics.persistence-enabled", havingValue = "false", matchIfMissing = true)
public class InMemoryMetricsLearningStore implements MetricsLearningStore {

    private final Map<String, EstimateVsActual> estimatesByRunId = new ConcurrentHashMap<>();
    private final List<EstimateVsActual> estimatesInsertionOrder = new ArrayList<>();

    private final Map<String, ThroughputProfile> throughputByRunId = new ConcurrentHashMap<>();
    private final List<ThroughputProfile> throughputInsertionOrder = new ArrayList<>();

    private final Map<String, ExecutionStatus> executionByRunId = new ConcurrentHashMap<>();
    private final List<ExecutionStatus> executionInsertionOrder = new ArrayList<>();

    @Override
    public void saveEstimateVsActual(EstimateVsActual record) {
        if (record == null || record.getRunId() == null) return;
        estimatesByRunId.put(record.getRunId(), record);
        synchronized (estimatesInsertionOrder) {
            estimatesInsertionOrder.add(record);
        }
    }

    @Override
    public List<EstimateVsActual> findEstimatesVsActualsByRunId(String runId) {
        EstimateVsActual one = estimatesByRunId.get(runId);
        return one != null ? List.of(one) : List.of();
    }

    @Override
    public List<EstimateVsActual> findRecentEstimatesVsActuals(String sourceType, String schemaName, String tableName, int limit) {
        List<EstimateVsActual> out = new ArrayList<>();
        synchronized (estimatesInsertionOrder) {
            for (int i = estimatesInsertionOrder.size() - 1; i >= 0 && out.size() < limit; i--) {
                EstimateVsActual r = estimatesInsertionOrder.get(i);
                out.add(r);
            }
        }
        out.sort(Comparator.comparing(EstimateVsActual::getRecordedAt).reversed());
        return out.stream().limit(limit).collect(Collectors.toList());
    }

    @Override
    public void saveThroughputProfile(ThroughputProfile profile) {
        if (profile == null || profile.getRunId() == null) return;
        throughputByRunId.put(profile.getRunId(), profile);
        synchronized (throughputInsertionOrder) {
            throughputInsertionOrder.add(profile);
        }
    }

    @Override
    public Optional<ThroughputProfile> findThroughputProfileByRunId(String runId) {
        return Optional.ofNullable(throughputByRunId.get(runId));
    }

    @Override
    public List<ThroughputProfile> findRecentThroughputProfiles(String sourceType, String schemaName, String tableName, int limit) {
        List<ThroughputProfile> out = new ArrayList<>();
        synchronized (throughputInsertionOrder) {
            for (int i = throughputInsertionOrder.size() - 1; i >= 0 && out.size() < limit; i--) {
                ThroughputProfile p = throughputInsertionOrder.get(i);
                if ((sourceType == null || sourceType.equals(p.getSourceType()))
                        && (schemaName == null || schemaName.equals(p.getSchemaName()))
                        && (tableName == null || tableName.equals(p.getTableName()))) {
                    out.add(p);
                }
            }
        }
        out.sort(Comparator.comparing(ThroughputProfile::getSampledAt).reversed());
        return out.stream().limit(limit).collect(Collectors.toList());
    }

    @Override
    public void saveExecutionStatus(ExecutionStatus status) {
        if (status == null || status.getRunId() == null) return;
        executionByRunId.put(status.getRunId(), status);
        synchronized (executionInsertionOrder) {
            executionInsertionOrder.add(status);
        }
    }

    @Override
    public void updateExecutionStatus(String runId, String status, java.time.Instant finishedAt, String jobId, String message) {
        ExecutionStatus existing = executionByRunId.get(runId);
        if (existing == null) return;
        ExecutionStatus updated = ExecutionStatus.builder()
                .runId(existing.getRunId())
                .profileRunId(existing.getProfileRunId())
                .mode(existing.getMode())
                .loadPattern(existing.getLoadPattern())
                .sourceType(existing.getSourceType())
                .schemaName(existing.getSchemaName())
                .tableName(existing.getTableName())
                .status(status != null ? status : existing.getStatus())
                .startedAt(existing.getStartedAt())
                .finishedAt(finishedAt != null ? finishedAt : existing.getFinishedAt())
                .createdAt(existing.getCreatedAt())
                .jobId(jobId != null ? jobId : existing.getJobId())
                .message(message != null ? message : existing.getMessage())
                .callerAgentId(existing.getCallerAgentId())
                .build();
        executionByRunId.put(runId, updated);
    }

    @Override
    public Optional<ExecutionStatus> findExecutionStatusByRunId(String runId) {
        return Optional.ofNullable(executionByRunId.get(runId));
    }

    @Override
    public List<ExecutionStatus> findRecentExecutionStatuses(int limit) {
        List<String> runIds = new ArrayList<>();
        synchronized (executionInsertionOrder) {
            for (int i = executionInsertionOrder.size() - 1; i >= 0 && runIds.size() < limit; i--) {
                runIds.add(executionInsertionOrder.get(i).getRunId());
            }
        }
        return runIds.stream()
                .map(executionByRunId::get)
                .filter(s -> s != null)
                .limit(limit)
                .collect(Collectors.toList());
    }

    @Override
    public List<ExecutionStatus> findExecutionStatusesByStatus(String status, int limit) {
        return executionByRunId.values().stream()
                .filter(s -> status.equals(s.getStatus()))
                .sorted(Comparator.comparing(ExecutionStatus::getCreatedAt).reversed())
                .limit(limit)
                .collect(Collectors.toList());
    }

    @Override
    public List<ExecutionStatus> findExecutionStatusesByCallerAgentId(String callerAgentId, int limit) {
        if (callerAgentId == null || callerAgentId.isBlank()) {
            return findRecentExecutionStatuses(limit);
        }
        return executionByRunId.values().stream()
                .filter(s -> callerAgentId.equals(s.getCallerAgentId()))
                .sorted(Comparator.comparing(ExecutionStatus::getCreatedAt).reversed())
                .limit(limit)
                .collect(Collectors.toList());
    }
}
