package com.di.streamnova.config;

import jakarta.validation.Valid;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

/**
 * List of pipeline actions (and optional event types) for one stage mode (2-stage, 3-stage, or validate-only).
 * YAML: pipeline.config.listeners.2-stage.actions / pipeline.config.listeners.3-stage.actions / pipeline.config.listeners.validate-only
 */
@Data
@NoArgsConstructor
public class StageActionsConfig {
    /** Optional stage tags (e.g. [VALIDATION] for validate-only). */
    private List<String> stages;
    @Valid
    private List<PipelineActionConfig> actions = new ArrayList<>();
}
