package com.di.streamnova.agent.workflow.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Response for GET/POST template-details: selected template and all calculated placeholder values.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class TemplateDetailsResponse {

    /** Workflow type label, e.g. db_to_gcs. */
    private String workflowType;
    /** Template resource path, e.g. classpath:pipeline-db-to-gcs-template.yml. */
    private String templatePath;
    /** All calculated values used to render the template (password masked). */
    private TemplateContextView context;
    /** When no template applies, explains why; null when context is present. */
    private String message;
}
