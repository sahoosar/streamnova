package com.di.streamnova.agent.profiler;

import java.util.List;
import java.util.Optional;

/**
 * Persistence for profile results. Used by Profiler to store runs and by Metrics & Learning Store
 * for estimates vs actuals and throughput profiles. Implementations can be in-memory, JDBC, or file-based.
 */
public interface ProfileStore {

    /**
     * Saves a profile result for later analysis and learning.
     *
     * @param result profile result from {@link ProfilerService#profile()} or {@link ProfilerService#profileWithConfig}
     * @return the run id that was stored
     */
    String save(ProfileResult result);

    /**
     * Loads a profile result by run id.
     */
    Optional<ProfileResult> findByRunId(String runId);

    /**
     * Returns the most recent profile results for a given source table (for learning and reporting).
     *
     * @param sourceType e.g. postgres
     * @param schemaName e.g. public
     * @param tableName  table name
     * @param limit      max number of results
     */
    List<ProfileResult> findRecentByTable(String sourceType, String schemaName, String tableName, int limit);
}
