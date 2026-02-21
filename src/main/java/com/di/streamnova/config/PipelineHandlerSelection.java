package com.di.streamnova.config;

import com.di.streamnova.handler.SourceHandler;
import lombok.Builder;
import lombok.Value;

/**
 * Resolved handler selection for the pipeline: one handler per stage (source; optional intermediate; destination).
 * Maps user/config choice to the actual handler instance(s) so execution is simple and modular.
 * <ul>
 *   <li>Two stages: source → destination (intermediate is null).</li>
 *   <li>Three stages: source → intermediate → destination.</li>
 * </ul>
 */
@Value
@Builder
public class PipelineHandlerSelection {

    /** Full load config used for this run (source, intermediate, target config). */
    LoadConfig loadConfig;

    /** Resolved handler types for each stage (source, intermediate, target). */
    PipelineHandlerRequirements requirements;

    /** The source handler instance to use for this run (e.g. PostgresHandler, OracleHandler, GCSHandler). */
    SourceHandler<?> sourceHandler;

    public String getSourceHandlerType() {
        return requirements != null ? requirements.getSourceHandlerType() : null;
    }

    public String getIntermediateHandlerType() {
        return requirements != null && requirements.hasIntermediate() ? requirements.getIntermediateHandlerType() : null;
    }

    public String getTargetHandlerType() {
        return requirements != null ? requirements.getTargetHandlerType() : null;
    }

    public boolean hasIntermediate() {
        return requirements != null && requirements.hasIntermediate();
    }
}
