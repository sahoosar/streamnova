package com.di.streamnova.agent.execution_engine;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * JSON body for 400 Bad Request when a handler in actions-mapping is not available.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ActionsMappingErrorResponse {
    private String message;
}
