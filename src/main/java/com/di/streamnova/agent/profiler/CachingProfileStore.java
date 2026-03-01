package com.di.streamnova.agent.profiler;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Cache-in-front of {@link JdbcProfileStore} when persistence and profile cache are enabled.
 * Lower/test: set {@code streamnova.profiler.cache-enabled=false} (no cache).
 * Higher env: set {@code streamnova.metrics.persistence-enabled=true} and {@code streamnova.profiler.cache-enabled=true}.
 */
@Service
@Primary
@ConditionalOnProperty(name = "streamnova.metrics.persistence-enabled", havingValue = "true")
@ConditionalOnProperty(name = "streamnova.profiler.cache-enabled", havingValue = "true")
public class CachingProfileStore implements ProfileStore {

    private final ProfileStore delegate;
    private final Cache<String, ProfileResult> byRunId;
    private final Cache<String, List<ProfileResult>> recentByTable;

    public CachingProfileStore(
            JdbcProfileStore delegate,
            @Value("${streamnova.profiler.cache.by-run-id.max-size:1000}") int byRunIdMaxSize,
            @Value("${streamnova.profiler.cache.by-run-id.expire-after-write-minutes:30}") int byRunIdExpireMinutes,
            @Value("${streamnova.profiler.cache.recent-by-table.max-size:500}") int recentByTableMaxSize,
            @Value("${streamnova.profiler.cache.recent-by-table.expire-after-write-minutes:5}") int recentByTableExpireMinutes) {
        this.delegate = delegate;
        this.byRunId = Caffeine.newBuilder()
                .maximumSize(byRunIdMaxSize)
                .expireAfterWrite(byRunIdExpireMinutes, TimeUnit.MINUTES)
                .build();
        this.recentByTable = Caffeine.newBuilder()
                .maximumSize(recentByTableMaxSize)
                .expireAfterWrite(recentByTableExpireMinutes, TimeUnit.MINUTES)
                .build();
    }

    @Override
    public String save(ProfileResult result) {
        String runId = delegate.save(result);
        if (runId != null) {
            byRunId.invalidate(runId);
            if (result != null && result.getTableProfile() != null) {
                String tableKey = tableKey(result.getTableProfile().getSourceType(),
                        result.getTableProfile().getSchemaName(),
                        result.getTableProfile().getTableName());
                recentByTable.asMap().keySet().removeIf(k -> k.startsWith(tableKey + "|"));
            }
        }
        return runId;
    }

    @Override
    public Optional<ProfileResult> findByRunId(String runId) {
        if (runId == null || runId.isBlank()) return Optional.empty();
        ProfileResult cached = byRunId.getIfPresent(runId);
        if (cached != null) return Optional.of(cached);
        Optional<ProfileResult> fromDb = delegate.findByRunId(runId);
        fromDb.ifPresent(r -> byRunId.put(runId, r));
        return fromDb;
    }

    @Override
    public List<ProfileResult> findRecentByTable(String sourceType, String schemaName, String tableName, int limit) {
        String key = tableKey(sourceType, schemaName, tableName) + "|" + limit;
        List<ProfileResult> cached = recentByTable.getIfPresent(key);
        if (cached != null) return cached;
        List<ProfileResult> fromDb = delegate.findRecentByTable(sourceType, schemaName, tableName, limit);
        recentByTable.put(key, fromDb);
        return fromDb;
    }

    private static String tableKey(String sourceType, String schemaName, String tableName) {
        return (sourceType != null ? sourceType : "") + "|" +
                (schemaName != null ? schemaName : "") + "|" +
                (tableName != null ? tableName : "");
    }
}
