package com.di.streamnova.guardrail;

import com.di.streamnova.agent.recommender.Guardrails;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Central guardrail service — the single place where {@link Guardrails} objects
 * are built and machine-type validation is performed.
 *
 * <p><b>Before this service existed</b>, guardrail logic was copy-pasted in three places:
 * <ul>
 *   <li>{@code RecommendationController} — {@code @Value allowedMachineTypesConfig} + {@code parseAllowedMachineTypes()}</li>
 *   <li>{@code ExecutionEngineService}   — {@code @Value allowedMachineTypesConfig} + {@code getAllowedMachineTypesList()}</li>
 *   <li>Ad-hoc inline in the request handler</li>
 * </ul>
 *
 * <p><b>After</b>: inject {@link GuardrailService} and call one of:
 * <ul>
 *   <li>{@link #buildFromConfig()} — guardrails from YAML only (no request overrides)</li>
 *   <li>{@link #buildWithOverrides(Double, Double, Double, List)} — YAML defaults + per-request params</li>
 *   <li>{@link #isMachineTypeAllowed(String)} — validate a single machine type</li>
 *   <li>{@link #assertMachineTypeAllowed(String)} — throw if not allowed (for execute path)</li>
 * </ul>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class GuardrailService {

    private final GuardrailProperties props;

    // ------------------------------------------------------------------ //
    // Build guardrails                                                    //
    // ------------------------------------------------------------------ //

    /**
     * Builds {@link Guardrails} purely from YAML configuration.
     * Returns {@code null} when no limits are configured (avoids unnecessary filtering).
     */
    public Guardrails buildFromConfig() {
        if (!props.isEnforceOnRecommend() || !props.hasAnyLimit()) return null;
        return build(
                props.getMaxCostUsd(),
                props.getMaxDurationSec(),
                props.getMinThroughputMbPerSec(),
                props.getAllowedMachineTypesList()
        );
    }

    /**
     * Builds {@link Guardrails} merging YAML defaults with per-request overrides.
     *
     * <p>Request parameters take precedence over YAML when provided.
     * Machine-type list from request replaces the YAML list when non-empty.
     *
     * @param maxCostUsdOverride          per-request cost cap (null = use YAML)
     * @param maxDurationSecOverride      per-request duration cap (null = use YAML)
     * @param minThroughputMbPerSecOverride per-request throughput floor (null = use YAML)
     * @param allowedMachineTypesOverride  per-request allowed types (null/empty = use YAML)
     */
    public Guardrails buildWithOverrides(
            Double maxCostUsdOverride,
            Double maxDurationSecOverride,
            Double minThroughputMbPerSecOverride,
            List<String> allowedMachineTypesOverride
    ) {
        Double effectiveCost       = firstFinitePositive(maxCostUsdOverride,     props.getMaxCostUsd());
        Double effectiveDuration   = firstFinitePositive(maxDurationSecOverride, props.getMaxDurationSec());
        Double effectiveThroughput = firstFinitePositive(minThroughputMbPerSecOverride, props.getMinThroughputMbPerSec());

        List<String> effectiveTypes =
                (allowedMachineTypesOverride != null && !allowedMachineTypesOverride.isEmpty())
                        ? allowedMachineTypesOverride
                        : props.getAllowedMachineTypesList();

        boolean hasAny = effectiveCost != null || effectiveDuration != null
                || effectiveThroughput != null || !effectiveTypes.isEmpty();

        if (!hasAny) return null;

        return build(effectiveCost, effectiveDuration, effectiveThroughput, effectiveTypes);
    }

    // ------------------------------------------------------------------ //
    // Machine-type validation                                             //
    // ------------------------------------------------------------------ //

    /**
     * Returns {@code true} when the machine type is allowed by the YAML config.
     * Always returns {@code true} when the allowed list is empty (= no restriction).
     */
    public boolean isMachineTypeAllowed(String machineType) {
        List<String> allowed = props.getAllowedMachineTypesList();
        return Guardrails.isMachineTypeAllowed(machineType, allowed);
    }

    /**
     * Throws {@link GuardrailViolationException} when machine type is not allowed
     * and {@code streamnova.guardrails.enforce-on-execute=true}.
     * No-op when enforcement is disabled or the allowed list is empty.
     */
    public void assertMachineTypeAllowed(String machineType) {
        if (!props.isEnforceOnExecute()) return;
        List<String> allowed = props.getAllowedMachineTypesList();
        if (allowed.isEmpty()) return;
        if (!Guardrails.isMachineTypeAllowed(machineType, allowed)) {
            String msg = "machineType '" + machineType + "' is not in the allowed list: " + allowed;
            log.warn("[GUARDRAIL] Rejected: {}", msg);
            throw new GuardrailViolationException(msg);
        }
    }

    /**
     * Returns the allowed machine types list from config (for logging/response).
     */
    public List<String> getAllowedMachineTypes() {
        return props.getAllowedMachineTypesList();
    }

    // ------------------------------------------------------------------ //
    // Private helpers                                                     //
    // ------------------------------------------------------------------ //

    private static Guardrails build(Double cost, Double duration,
                                    Double throughput, List<String> types) {
        return Guardrails.builder()
                .maxCostUsd(cost)
                .maxDurationSec(duration)
                .minThroughputMbPerSec(throughput)
                .allowedMachineTypes(types.isEmpty() ? null : types)
                .build();
    }

    /**
     * Returns the first value that is non-null, finite, and positive; otherwise null.
     */
    private static Double firstFinitePositive(Double... values) {
        for (Double v : values) {
            if (v != null && Double.isFinite(v) && v > 0) return v;
        }
        return null;
    }
}
