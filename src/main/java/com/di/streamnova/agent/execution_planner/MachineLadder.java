package com.di.streamnova.agent.execution_planner;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Ordered ladder of GCP machine types for candidate generation (n2d → n2 → c3 by default).
 * Types and order are read from {@link MachineLadderProperties}; add or change machine types
 * via configuration without modifying this class.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class MachineLadder {

    private final MachineLadderProperties properties;

    /**
     * Returns the full machine ladder in configured order.
     */
    public List<String> getDefaultLadder() {
        return properties.getTypes();
    }

    /**
     * Returns a ladder filtered by family: "n2" (n2 only, not n2d), "n2d", "c3".
     * If prefix is null or blank, returns the full default ladder.
     */
    public List<String> getLadder(String familyPrefix) {
        List<String> all = properties.getTypes();
        if (familyPrefix == null || familyPrefix.isBlank()) {
            return all;
        }
        String p = familyPrefix.trim().toLowerCase();
        if ("n2".equals(p)) return getN2Ladder();
        if ("n2d".equals(p)) return getN2dLadder();
        if ("c3".equals(p)) return getC3Ladder();
        List<String> out = all.stream()
                .filter(mt -> mt.toLowerCase().startsWith(p))
                .collect(Collectors.toList());
        return out.isEmpty() ? all : Collections.unmodifiableList(out);
    }

    /**
     * Returns machine types in a given vCPU tier (e.g. 4, 8, 16).
     */
    public List<String> getLadderByVcpuTier(int vCpus) {
        return properties.getTypes().stream()
                .filter(mt -> extractVcpus(mt) == vCpus)
                .collect(Collectors.toList());
    }

    /** Extracts vCPU count from machine type string (e.g. n2-standard-8 → 8). */
    public static int extractVcpus(String machineType) {
        if (machineType == null || machineType.isBlank()) return 0;
        String[] parts = machineType.split("-");
        if (parts.length == 0) return 0;
        try {
            int v = Integer.parseInt(parts[parts.length - 1]);
            return v > 0 ? v : 0;
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    /** n2 only (excludes n2d): standard, highmem, highcpu. */
    public List<String> getN2Ladder() {
        return properties.getTypes().stream()
                .filter(mt -> mt.toLowerCase().startsWith("n2-") && !mt.toLowerCase().startsWith("n2d-"))
                .collect(Collectors.toList());
    }

    /** n2d only. */
    public List<String> getN2dLadder() {
        return properties.getTypes().stream()
                .filter(mt -> mt.toLowerCase().startsWith("n2d-"))
                .collect(Collectors.toList());
    }

    /** c3 only. */
    public List<String> getC3Ladder() {
        return properties.getTypes().stream()
                .filter(mt -> mt.toLowerCase().startsWith("c3-"))
                .collect(Collectors.toList());
    }

    /**
     * Returns the ladder in configured order (same as getDefaultLadder()).
     */
    public List<String> getLadderByFamilyOrder() {
        return getDefaultLadder();
    }
}
