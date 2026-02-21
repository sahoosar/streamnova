package com.di.streamnova.agent.execution_engine;

import com.di.streamnova.agent.workflow.dto.TemplateDetailsRequest;
import com.di.streamnova.agent.workflow.dto.TemplateDetailsResponse;
import com.di.streamnova.config.PipelineListenerMappingService;
import com.di.streamnova.agent.workflow.PipelineTemplateService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * API to read pipeline listener config (2-stage or 3-stage), map actions to user-selected handlers,
 * and return JSON. Second endpoint creates datasources per the mapping and returns connection details per stage.
 */
@RestController
@RequestMapping("/api/agent/pipeline-listener")
@RequiredArgsConstructor
public class PipelineListenerController {

    private final PipelineListenerMappingService pipelineListenerMappingService;
    private final PipelineTemplateService pipelineTemplateService;

    /**
     * Returns pipeline_listener_config details. Omit query param to get all modes (2-stage, 3-stage, validate-only, etc.);
     * use stage=2 or stage=3 to get only that stage's details. Returns 400 if stage is not 2 or 3.
     */
    @GetMapping(value = "/stages-mapping", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> getStagesMapping(
            @RequestParam(required = false) Integer stage) {
        if (stage != null && stage != 2 && stage != 3) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(StagesMappingErrorResponse.invalidStage());
        }
        StagesMappingResponse response = pipelineListenerMappingService.getStagesMapping(stage);
        return ResponseEntity.ok(response);
    }

    /**
     * Returns actions mapping: reads listener config for 2-stage or 3-stage (based on whether intermediate is provided),
     * maps SOURCE_READ to source handler, TARGET_WRITE to target handler, and INTERMEDIATE_WRITE (3-stage) to intermediate handler.
     * Response is JSON with stages, mode, and actionsMapping (action, handler, eventType).
     */
    @GetMapping(value = "/actions-mapping", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<ActionsMappingResponse> getActionsMapping(
            @RequestParam(required = false) String source,
            @RequestParam(required = false) String intermediate,
            @RequestParam(required = false) String target) {
        ActionsMappingResponse response = pipelineListenerMappingService.getActionsMapping(source, intermediate, target);
        return ResponseEntity.ok(response);
    }

    /**
     * Returns connection details per listener stage. When use-event-configs-only, also creates
     * JDBC connection pool for SOURCE_READ (postgres) so the pipeline can use it.
     * GET: use query params ?source=postgres&intermediate=gcs&target=bq
     */
    @GetMapping(value = "/datasource-details", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<DatasourceConnectionDetailsResponse> getDatasourceDetails(
            @RequestParam(required = false) String source,
            @RequestParam(required = false) String intermediate,
            @RequestParam(required = false) String target) {
        DatasourceConnectionDetailsResponse response = pipelineListenerMappingService.getDatasourceConnectionDetails(source, intermediate, target);
        return ResponseEntity.ok(response);
    }

    /**
     * Same as GET /datasource-details but accepts JSON body: { "source": "postgres", "intermediate": "gcs", "target": "bq" }.
     * Returns connection details per listener stage; ensures JDBC pool for SOURCE_READ when applicable.
     */
    @PostMapping(value = "/datasource-details", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<DatasourceConnectionDetailsResponse> getDatasourceDetailsPost(@RequestBody(required = false) CreateDatasourceRequest body) {
        String source = body != null ? body.getSource() : null;
        String intermediate = body != null ? body.getIntermediate() : null;
        String target = body != null ? body.getTarget() : null;
        DatasourceConnectionDetailsResponse response = pipelineListenerMappingService.getDatasourceConnectionDetails(source, intermediate, target);
        return ResponseEntity.ok(response);
    }

    /**
     * Creates datasource(s) and connection pool(s) for all listener events that need one.
     * Request body: { "source": "postgres", "target": "gcs" } for 2-stage; add "intermediate": "gcs" for 3-stage.
     * Returns success true only if every required pool was created. Then call POST /api/agent/execute with the same body.
     */
    @PostMapping(value = "/create-datasource", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<CreateDatasourceResponse> createDatasource(@RequestBody(required = false) CreateDatasourceRequest body) {
        String source = body != null ? body.getSource() : null;
        String intermediate = body != null ? body.getIntermediate() : null;
        String target = body != null ? body.getTarget() : null;
        CreateDatasourceResponse response = pipelineListenerMappingService.createDatasourcePoolsForMapping(source, intermediate, target);
        return ResponseEntity.ok(response);
    }

    /**
     * Returns all calculated template values for the given source/target (and optional intermediate, candidate, schema/table).
     * Use this to preview the resolved placeholders before executing. Password is masked in the response.
     * POST body: { "source": "postgres", "target": "gcs", "candidate": { ... }, "source_table": "...", ... }
     */
    @PostMapping(value = "/template-details", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<TemplateDetailsResponse> getTemplateDetails(@RequestBody(required = false) TemplateDetailsRequest body) {
        TemplateDetailsResponse response = pipelineTemplateService.getTemplateDetails(body);
        return ResponseEntity.ok(response);
    }

    /**
     * Returns template details (calculated values) for the given source and target via query params.
     * Optional: intermediate, and defaults are used for workers/shards when not provided.
     */
    @GetMapping(value = "/template-details", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<TemplateDetailsResponse> getTemplateDetailsByParams(
            @RequestParam(required = true) String source,
            @RequestParam(required = true) String target,
            @RequestParam(required = false) String intermediate) {
        TemplateDetailsRequest request = new TemplateDetailsRequest();
        request.setSource(source);
        request.setTarget(target);
        request.setIntermediate(intermediate);
        TemplateDetailsResponse response = pipelineTemplateService.getTemplateDetails(request);
        return ResponseEntity.ok(response);
    }

    /**
     * Returns 400 when a handler in actions-mapping is not in available/configured handlers (pipeline-handlers).
     */
    @ExceptionHandler(HandlerValidationException.class)
    public ResponseEntity<ActionsMappingErrorResponse> handleHandlerValidation(HandlerValidationException ex) {
        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(new ActionsMappingErrorResponse(ex.getMessage()));
    }
}
