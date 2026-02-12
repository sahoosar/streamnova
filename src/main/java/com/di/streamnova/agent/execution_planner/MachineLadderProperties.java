package com.di.streamnova.agent.execution_planner;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Configurable machine ladder for candidate generation.
 * When {@link #getTypes()} is empty or not set, returns the default ladder (n2d → n2 → c3).
 * Override via: streamnova.execution-planner.machine-ladder.types=n2d-standard-4,n2d-standard-8,...
 */
@ConfigurationProperties(prefix = "streamnova.execution-planner.machine-ladder")
public class MachineLadderProperties {

    /**
     * Ordered list of GCP machine types. Empty or unset = use default ladder.
     * Example: n2d-standard-4,n2d-standard-8,n2d-standard-16,n2-standard-4,...,c3-standard-16
     */
    private List<String> types = new ArrayList<>();

    public void setTypes(List<String> types) {
        this.types = types != null ? new ArrayList<>(types) : new ArrayList<>();
    }

    /**
     * Returns the configured types, or the default ladder if not set or empty.
     */
    public List<String> getTypes() {
        if (types != null && !types.isEmpty()) {
            return Collections.unmodifiableList(types);
        }
        return getDefaultTypes();
    }

    private static List<String> getDefaultTypes() {
        List<String> ladder = new ArrayList<>();
        for (int v : new int[] { 4, 8, 16 }) ladder.add("n2d-standard-" + v);
        for (int v : new int[] { 4, 8, 16 }) ladder.add("n2-standard-" + v);
        for (int v : new int[] { 4, 8, 16 }) ladder.add("n2-highmem-" + v);
        for (int v : new int[] { 4, 8, 16 }) ladder.add("n2-highcpu-" + v);
        for (int v : new int[] { 4, 8, 16 }) ladder.add("c3-standard-" + v);
        return Collections.unmodifiableList(ladder);
    }
}
