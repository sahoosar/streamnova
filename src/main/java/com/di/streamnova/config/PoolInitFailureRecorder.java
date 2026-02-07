package com.di.streamnova.config;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Records pool creation failures per source at startup so pool-statistics can show all failed sources (e.g. postgres + oracle).
 */
public final class PoolInitFailureRecorder {

    private static final Map<String, String> failuresBySource = new ConcurrentHashMap<>();

    public static void setLastFailure(String sourceKey, String message) {
        if (sourceKey != null && message != null) {
            failuresBySource.put(sourceKey, message);
        }
    }

    /** Clear recorded failures (e.g. before a new startup init run). */
    public static void clearFailures() {
        failuresBySource.clear();
    }

    /** Returns the last recorded failure message (backward compat). */
    public static String getLastFailureMessage() {
        Map<String, String> all = getAllFailures();
        return all.isEmpty() ? null : all.values().iterator().next();
    }

    /** Returns all failures by source key (e.g. postgres -> msg, oracle -> msg). */
    public static Map<String, String> getAllFailures() {
        return Collections.unmodifiableMap(new LinkedHashMap<>(failuresBySource));
    }

    /** Returns a single string for display, e.g. "postgres: msg1; oracle: msg2". */
    public static String getAllFailuresFormatted() {
        Map<String, String> all = getAllFailures();
        if (all.isEmpty()) return null;
        return all.entrySet().stream()
                .map(e -> e.getKey() + ": " + e.getValue())
                .collect(Collectors.joining("; "));
    }

    private PoolInitFailureRecorder() {}
}
