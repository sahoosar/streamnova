package com.di.streamnova.guardrail;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * Single, authoritative binding for ALL guardrail configuration.
 *
 * <p>Every guardrail value is driven entirely from YAML — no scattered {@code @Value}
 * annotations in controllers or services. To change any limit, edit the relevant
 * {@code application-{profile}.yml} and redeploy. No code change required.
 *
 * <pre>
 * streamnova:
 *   guardrails:
 *     allowed-machine-types: n2d-standard-4,n2-standard-4,...
 *     max-duration-sec: 7200
 *     max-cost-usd: 5.00
 *     min-throughput-mb-per-sec: 10
 *     machine-family-order: n2d,n2,c3
 *     max-workers-global: 8
 *     max-shards-global: 32
 *     enforce-on-execute: true
 *     enforce-on-recommend: true
 * </pre>
 */
@Data
@Component
@ConfigurationProperties(prefix = "streamnova.guardrails")
public class GuardrailProperties {

    // ------------------------------------------------------------------ //
    // Machine type restrictions                                           //
    // ------------------------------------------------------------------ //

    /**
     * Comma-separated list of allowed GCP machine types.
     * Supports exact match ("n2d-standard-4") or family prefix ("n2d" → allows all n2d-*).
     * Empty = no restriction.
     */
    private String allowedMachineTypes = "";

    /**
     * Ordered machine family preference used when no explicit type is given.
     * First match wins — keep n2d before n2 so n2d-* is preferred.
     */
    private String machineFamilyOrder = "n2d,n2,c3";

    // ------------------------------------------------------------------ //
    // Cost / duration / throughput limits (null = no limit)              //
    // ------------------------------------------------------------------ //

    /** Maximum allowed estimated cost in USD. Null = no cost guardrail. */
    private Double maxCostUsd;

    /** Maximum allowed estimated duration in seconds. Null = no duration guardrail. */
    private Double maxDurationSec;

    /** Minimum required effective throughput in MB/s. Null = no throughput guardrail. */
    private Double minThroughputMbPerSec;

    // ------------------------------------------------------------------ //
    // Worker / shard caps                                                 //
    // ------------------------------------------------------------------ //

    /**
     * Global hard cap on the number of Dataflow workers across all candidates.
     * 0 = use streamnova.recommend.max-workers-cap instead.
     */
    private int maxWorkersGlobal = 0;

    /**
     * Global hard cap on shard count per execution.
     * 0 = derive from pool size.
     */
    private int maxShardsGlobal = 0;

    /**
     * Max concurrent load runs (shared pool). When a run is not exclusive, it uses one "run slot".
     * 0 = no cap (only pool size limits concurrency). E.g. 2 = at most 2 loads at a time (more 429s, shorter per-load time).
     */
    private int maxConcurrentRuns = 0;

    // ------------------------------------------------------------------ //
    // Enforcement switches                                                //
    // ------------------------------------------------------------------ //

    /**
     * When true (default), guardrails are applied during GET /api/agent/recommend.
     * Set false to disable guardrail filtering on recommendations without removing the config.
     */
    private boolean enforceOnRecommend = true;

    /**
     * When true (default), machine-type guardrail is enforced at POST /api/agent/execute.
     * Prevents execution of a candidate that bypasses the recommendation step.
     */
    private boolean enforceOnExecute = true;

    // ------------------------------------------------------------------ //
    // Convenience helpers                                                 //
    // ------------------------------------------------------------------ //

    /**
     * Parses {@link #allowedMachineTypes} into a trimmed, non-empty list.
     * Returns an empty list when the property is blank (= no restriction).
     */
    public List<String> getAllowedMachineTypesList() {
        if (allowedMachineTypes == null || allowedMachineTypes.isBlank()) return List.of();
        List<String> out = new ArrayList<>();
        for (String t : allowedMachineTypes.split(",")) {
            String trimmed = t.trim();
            if (!trimmed.isEmpty()) out.add(trimmed);
        }
        return out;
    }

    /**
     * Parses {@link #machineFamilyOrder} into an ordered list of family prefixes.
     */
    public List<String> getMachineFamilyOrderList() {
        if (machineFamilyOrder == null || machineFamilyOrder.isBlank())
            return List.of("n2d", "n2", "c3");
        List<String> out = new ArrayList<>();
        for (String f : machineFamilyOrder.split(",")) {
            String trimmed = f.trim();
            if (!trimmed.isEmpty()) out.add(trimmed);
        }
        return out;
    }

    /** Returns true when any guardrail limit is actually configured. */
    public boolean hasAnyLimit() {
        return maxCostUsd != null
                || maxDurationSec != null
                || minThroughputMbPerSec != null
                || !getAllowedMachineTypesList().isEmpty();
    }
}
