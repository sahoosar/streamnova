package com.di.streamnova.agent.execution_planner;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Ordered ladder of GCP machine types for candidate generation: n2 → n2d → c3.
 * Used by Candidate Generator to propose machine types for the Estimator to score.
 */
@Slf4j
public final class MachineLadder {

    private static final List<String> DEFAULT_LADDER = buildDefaultLadder();

    private MachineLadder() {}

    /**
     * Returns the default machine ladder: n2 (standard, highmem, highcpu), then n2d, then c3.
     * Sizes include 4, 8, 16 vCPUs per family where applicable.
     */
    public static List<String> getDefaultLadder() {
        return Collections.unmodifiableList(DEFAULT_LADDER);
    }

    /**
     * Returns a ladder filtered by family: "n2" (n2 only, not n2d), "n2d", "c3".
     * If prefix is null or blank, returns the full default ladder.
     */
    public static List<String> getLadder(String familyPrefix) {
        if (familyPrefix == null || familyPrefix.isBlank()) {
            return getDefaultLadder();
        }
        String p = familyPrefix.trim().toLowerCase();
        if ("n2".equals(p)) return getN2Ladder();
        if ("n2d".equals(p)) return getN2dLadder();
        if ("c3".equals(p)) return getC3Ladder();
        List<String> out = new ArrayList<>();
        for (String mt : DEFAULT_LADDER) {
            if (mt.toLowerCase().startsWith(p)) out.add(mt);
        }
        return out.isEmpty() ? getDefaultLadder() : Collections.unmodifiableList(out);
    }

    /**
     * Returns machine types in a given vCPU tier (e.g. 4, 8, 16).
     */
    public static List<String> getLadderByVcpuTier(int vCpus) {
        List<String> out = new ArrayList<>();
        for (String mt : DEFAULT_LADDER) {
            if (extractVcpus(mt) == vCpus) {
                out.add(mt);
            }
        }
        return Collections.unmodifiableList(out);
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

    /** n2 only (excludes n2d): standard, highmem, highcpu. Check all n2 first, then move to others. */
    private static final List<String> N2_LADDER = buildN2OnlyLadder();
    /** n2d only. */
    private static final List<String> N2D_LADDER = buildN2dLadder();
    /** c3 only. */
    private static final List<String> C3_LADDER = buildC3Ladder();

    /**
     * Returns only n2 series (standard, highmem, highcpu), excluding n2d. Use this to exhaust n2 first.
     */
    public static List<String> getN2Ladder() {
        return Collections.unmodifiableList(N2_LADDER);
    }

    /**
     * Returns only n2d series. Used after n2 when moving to other families.
     */
    public static List<String> getN2dLadder() {
        return Collections.unmodifiableList(N2D_LADDER);
    }

    /**
     * Returns only c3 series. Used after n2 and n2d.
     */
    public static List<String> getC3Ladder() {
        return Collections.unmodifiableList(C3_LADDER);
    }

    private static List<String> buildN2OnlyLadder() {
        List<String> ladder = new ArrayList<>();
        for (int v : new int[] { 4, 8, 16 }) ladder.add("n2-standard-" + v);
        for (int v : new int[] { 4, 8, 16 }) ladder.add("n2-highmem-" + v);
        for (int v : new int[] { 4, 8, 16 }) ladder.add("n2-highcpu-" + v);
        return ladder;
    }

    private static List<String> buildN2dLadder() {
        List<String> ladder = new ArrayList<>();
        for (int v : new int[] { 4, 8, 16 }) ladder.add("n2d-standard-" + v);
        return ladder;
    }

    private static List<String> buildC3Ladder() {
        List<String> ladder = new ArrayList<>();
        for (int v : new int[] { 4, 8, 22 }) ladder.add("c3-standard-" + v);
        return ladder;
    }

    /**
     * Returns the ladder in family order: first all n2, then all n2d, then all c3.
     * Candidate generation uses this order so n2 series is fully considered before moving to others.
     */
    public static List<String> getLadderByFamilyOrder() {
        return getDefaultLadder();
    }

    private static List<String> buildDefaultLadder() {
        List<String> ladder = new ArrayList<>();
        // Tier 1: n2 only (standard, then highmem, then highcpu) — checked first
        for (int v : new int[] { 4, 8, 16 }) {
            ladder.add("n2-standard-" + v);
        }
        for (int v : new int[] { 4, 8, 16 }) {
            ladder.add("n2-highmem-" + v);
        }
        for (int v : new int[] { 4, 8, 16 }) {
            ladder.add("n2-highcpu-" + v);
        }
        // Tier 2: n2d — only after all n2 considered
        for (int v : new int[] { 4, 8, 16 }) {
            ladder.add("n2d-standard-" + v);
        }
        // Tier 3: c3 — last
        for (int v : new int[] { 4, 8, 22 }) {
            ladder.add("c3-standard-" + v);
        }
        log.debug("[CANDIDATES] Machine ladder: {} types (n2 first, then n2d, then c3)", ladder.size());
        return ladder;
    }
}
