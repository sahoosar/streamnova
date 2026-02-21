package com.di.streamnova.config;

import lombok.Builder;
import lombok.Value;

/**
 * Optional overrides for pipeline handler selection (e.g. from agent execute request).
 * Lets the user choose which handler to use for each stage when multiple are present.
 * <ul>
 *   <li>Two handlers: source + target (set source, target).</li>
 *   <li>Three handlers: source + intermediate + target (set all three).</li>
 * </ul>
 * Unset values fall back to config (defaultSource, intermediate, target from YAML).
 */
@Value
@Builder
public class HandlerOverrides {

    /** Override source connection/type (e.g. postgres, oracle, gcs). */
    String source;
    /** Override intermediate handler type (e.g. gcs). */
    String intermediate;
    /** Override target handler type (e.g. bigquery). */
    String target;

    public static HandlerOverrides none() {
        return HandlerOverrides.builder().build();
    }

    public static HandlerOverrides sourceOnly(String sourceKey) {
        return HandlerOverrides.builder().source(blankToNull(sourceKey)).build();
    }

    private static String blankToNull(String s) {
        return s != null && !s.isBlank() ? s.trim() : null;
    }
}
