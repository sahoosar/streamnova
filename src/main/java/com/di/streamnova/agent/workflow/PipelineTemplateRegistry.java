package com.di.streamnova.agent.workflow;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Maps (source, intermediate, target) to a pipeline template resource path.
 * Used to select which template YAML to render for execution (e.g. db-to-gcs, db-to-gcs-to-bq).
 * <p>
 * Registered templates (application.yml streamnova.pipeline.templates): db-to-gcs, db-to-gcs-to-bq, gcs-to-bq, gcs-to-gcs,
 * warehouse-to-gcs-to-bq (hive/impala → gcs → bigquery), bq-to-gcs (bigquery → gcs).
 */
@Slf4j
@Component
public class PipelineTemplateRegistry {

    /** Workflow type returned when the selected template path does not match any known pattern (e.g. a custom template). */
    public static final String WORKFLOW_TYPE_CUSTOM = "custom";

    /** From application.yml: streamnova.pipeline.templates.* */
    @Value("${streamnova.pipeline.templates.db-to-gcs:}")
    private String dbToGcsTemplatePath;
    @Value("${streamnova.pipeline.templates.db-to-gcs-to-bq:}")
    private String dbToGcsToBqTemplatePath;
    @Value("${streamnova.pipeline.templates.gcs-to-bq:}")
    private String gcsToBqTemplatePath;
    @Value("${streamnova.pipeline.templates.gcs-to-gcs:}")
    private String gcsToGcsTemplatePath;
    @Value("${streamnova.pipeline.templates.warehouse-to-gcs-to-bq:}")
    private String warehouseToGcsToBqTemplatePath;
    @Value("${streamnova.pipeline.templates.bq-to-gcs:}")
    private String bqToGcsTemplatePath;

    private final Map<String, String> cache = new ConcurrentHashMap<>();

    /**
     * Returns the template resource path for the given handler keys, or null if no template is defined.
     * 2-stage: (source, null, target). 3-stage: (source, intermediate, target).
     */
    public String selectTemplate(String sourceKey, String intermediateKey, String targetKey) {
        if (sourceKey == null || sourceKey.isBlank() || targetKey == null || targetKey.isBlank()) {
            return null;
        }
        String src = sourceKey.trim().toLowerCase();
        String tgt = targetKey.trim().toLowerCase();
        String mid = (intermediateKey != null && !intermediateKey.isBlank()) ? intermediateKey.trim().toLowerCase() : null;

        String cacheKey = src + "|" + (mid != null ? mid : "") + "|" + tgt;
        return cache.computeIfAbsent(cacheKey, k -> resolveTemplatePath(src, mid, tgt));
    }

    private String resolveTemplatePath(String source, String intermediate, String target) {
        boolean dbSource = "postgres".equals(source) || "oracle".equals(source);
        boolean gcsTarget = "gcs".equals(target);
        boolean bqTarget = "bigquery".equals(target);
        boolean gcsIntermediate = "gcs".equals(intermediate);

        if (dbSource && gcsTarget && intermediate == null) {
            return blankToNull(dbToGcsTemplatePath);
        }
        if (dbSource && bqTarget && gcsIntermediate) {
            return blankToNull(dbToGcsToBqTemplatePath);
        }
        if ("gcs".equals(source) && bqTarget && intermediate == null) {
            return blankToNull(gcsToBqTemplatePath);
        }
        if ("gcs".equals(source) && gcsTarget && intermediate == null) {
            return blankToNull(gcsToGcsTemplatePath);
        }
        if (("hive".equals(source) || "impala".equals(source)) && bqTarget && gcsIntermediate) {
            return blankToNull(warehouseToGcsToBqTemplatePath);
        }
        if ("bigquery".equals(source) && gcsTarget && intermediate == null) {
            return blankToNull(bqToGcsTemplatePath);
        }

        log.debug("[TEMPLATE] No template for source={}, intermediate={}, target={}", source, intermediate, target);
        return null;
    }

    /**
     * Returns a workflow type label for the API (e.g. template-details, actions-mapping) based on the
     * template selected for the given (source, intermediate, target).
     * <p>
     * Uses the same keys as {@link #selectTemplate(String, String, String)} to resolve the template path,
     * then derives the label from the path:
     * <ul>
     *   <li>path contains {@code db-to-gcs-template} and not {@code to-bq} → {@code "db_to_gcs"}</li>
     *   <li>path contains {@code db-to-gcs-to-bq} → {@code "db_to_gcs_to_bq"}</li>
     *   <li>path contains {@code gcs-to-bq} → {@code "gcs_to_bq"}</li>
     *   <li>path contains {@code gcs-to-gcs} → {@code "gcs_to_gcs"}</li>
     *   <li>path contains {@code warehouse-to-gcs-to-bq} → {@code "warehouse_to_gcs_to_bq"}</li>
     *   <li>path contains {@code bq-to-gcs} → {@code "bq_to_gcs"}</li>
     *   <li>otherwise → null (unknown template path)</li>
     * </ul>
     * Returns null if source or target key is null/blank, or if no template is selected, or if the path does not match a known workflow.
     *
     * @param sourceKey      source handler key (e.g. postgres, hive, bigquery); must be non-blank
     * @param intermediateKey intermediate key for 3-stage, or null for 2-stage
     * @param targetKey      target handler key (e.g. gcs, bigquery); must be non-blank
     * @return workflow type string for API, or null if invalid input, no template, or unknown path
     */
    public String getWorkflowType(String sourceKey, String intermediateKey, String targetKey) {
        if (sourceKey == null || sourceKey.isBlank() || targetKey == null || targetKey.isBlank()) {
            return null;
        }
        String path = selectTemplate(sourceKey, intermediateKey, targetKey);
        if (path == null) return null;
        if (path.contains("db-to-gcs-template") && !path.contains("to-bq")) return "db_to_gcs";
        if (path.contains("db-to-gcs-to-bq")) return "db_to_gcs_to_bq";
        if (path.contains("gcs-to-bq")) return "gcs_to_bq";
        if (path.contains("gcs-to-gcs")) return "gcs_to_gcs";
        if (path.contains("warehouse-to-gcs-to-bq")) return "warehouse_to_gcs_to_bq";
        if (path.contains("bq-to-gcs")) return "bq_to_gcs";
        return null;
    }

    private static String blankToNull(String s) {
        return s != null && !s.isBlank() ? s.trim() : null;
    }
}
