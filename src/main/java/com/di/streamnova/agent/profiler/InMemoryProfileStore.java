package com.di.streamnova.agent.profiler;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory implementation of {@link ProfileStore}. Suitable for single-node and testing.
 * When {@code streamnova.metrics.persistence-enabled=true}, {@link JdbcProfileStore} is used instead.
 */
@Component
@ConditionalOnProperty(name = "streamnova.metrics.persistence-enabled", havingValue = "false", matchIfMissing = true)
public class InMemoryProfileStore implements ProfileStore {

    private final Map<String, ProfileResult> byRunId = new ConcurrentHashMap<>();
    private final List<ProfileResult> insertionOrder = new ArrayList<>();

    @Override
    public String save(ProfileResult result) {
        if (result == null || result.getRunId() == null) return null;
        byRunId.put(result.getRunId(), result);
        synchronized (insertionOrder) {
            insertionOrder.add(result);
        }
        return result.getRunId();
    }

    @Override
    public Optional<ProfileResult> findByRunId(String runId) {
        return Optional.ofNullable(byRunId.get(runId));
    }

    @Override
    public List<ProfileResult> findRecentByTable(String sourceType, String schemaName, String tableName, int limit) {
        List<ProfileResult> out = new ArrayList<>();
        synchronized (insertionOrder) {
            for (int i = insertionOrder.size() - 1; i >= 0 && out.size() < limit; i--) {
                ProfileResult r = insertionOrder.get(i);
                TableProfile p = r.getTableProfile();
                if (p != null
                        && (sourceType == null || sourceType.equals(p.getSourceType()))
                        && (schemaName == null || schemaName.equals(p.getSchemaName()))
                        && (tableName == null || tableName.equals(p.getTableName()))) {
                    out.add(r);
                }
            }
        }
        out.sort(Comparator.comparing(ProfileResult::getCompletedAt).reversed());
        return out;
    }
}
