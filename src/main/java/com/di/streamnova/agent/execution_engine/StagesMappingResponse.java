package com.di.streamnova.agent.execution_engine;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * JSON response for GET /api/agent/pipeline-listener/stages-mapping.
 * Returns pipeline_listener_config details for the requested stage(s).
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class StagesMappingResponse {
    /** Listener config file path (entrypoint), e.g. pipeline_listener_config.yml. */
    private String entrypoint;
    /** Stage details: one item when ?stage=2 or ?stage=3, or both when no param. */
    private List<StageDetailDto> stages;
    /** Optional message. */
    private String message;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class StageDetailDto {
        /** Stage value: 2, 3, or 0 for other modes (e.g. validate-only). */
        private int stageValue;
        /** Mode label: "2-stage", "3-stage", or "validate-only". */
        private String mode;
        /** Optional stage tags from YAML (e.g. [VALIDATION] for validate-only). */
        private List<String> stages;
        /** Actions for this stage (action name + eventType from YAML). */
        private List<StageActionEntry> actions;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class StageActionEntry {
        private String action;
        private String eventType;
    }
}
