package com.di.streamnova.agent.execution_engine;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Collections;
import java.util.List;

/**
 * JSON body for 400 Bad Request when stage param is not 2 or 3.
 * Guarantees a clean, production-style response with no null or extra fields.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class StagesMappingErrorResponse {
    private List<?> stages = Collections.emptyList();
    private String message;

    public static StagesMappingErrorResponse invalidStage() {
        return new StagesMappingErrorResponse(
                Collections.emptyList(),
                "Only 2-stage and 3-stage are supported. Use stage=2 or stage=3, or omit the parameter for all stages."
        );
    }
}
