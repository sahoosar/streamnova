package com.di.streamnova.config;

import jakarta.validation.Valid;
import lombok.Data;

import java.util.Map;

/**
 * Root pipeline config loaded from YAML (e.g. database_event_config.yml, or a single pipeline config file when not event-configs-only).
 * Structure is segregated into three handler-type sections, each usable independently:
 * <ul>
 *   <li><b>Source</b>: {@link #sources} (or legacy {@link #connections}) — named by handler type (postgres, oracle, gcs).</li>
 *   <li><b>Intermediate</b>: {@link #intermediates} (or legacy {@link #intermediate}) — named by handler type (gcs, bigquery).</li>
 *   <li><b>Destination</b>: {@link #destinations} (or legacy {@link #target}) — named by handler type (bigquery, gcs).</li>
 * </ul>
 * Use defaultSource / defaultIntermediate / defaultDestination (or API overrides) to select which config to use per stage.
 */
@Data
public class LoadConfig {

    // ----- Source: single or named handler-type configs -----
    /** Single source when one DB profile (e.g. only postgres). Legacy; prefer sources/connections + tables. */
    @Valid
    private PipelineConfigSource source;

    /**
     * Named source configs by handler type or logical name (e.g. postgres, oracle, gcs).
     * YAML: pipeline.config.sources.postgres / .oracle / .gcs
     * When set, used instead of {@link #connections}; select via {@link #defaultSource}.
     */
    private Map<String, @Valid PipelineConfigSource> sources;

    /**
     * Named DB connections (legacy). Keys: connection name (e.g. postgres, oracle).
     * YAML: pipeline.config.connections.postgres / .oracle
     * When {@link #sources} is not set, connections are used as source configs.
     */
    private Map<String, @Valid PipelineConfigSource> connections;

    /**
     * Per-table config: key = logical name (API ?source=key), value = source/connection name + table-specific fields.
     * Table config uses "connection" to reference a key in sources or connections.
     * YAML: pipeline.config.tables.market_summary with connection + table (and optional maxColumns, shardColumn, etc.).
     */
    private Map<String, @Valid TableConfig> tables;

    /** Default table key when no ?source= given. If set, default request uses this table (source + table merged). */
    private String defaultTable;
    /** Default source key when no ?source= and defaultTable not set (e.g. postgres, oracle, gcs). */
    private String defaultSource;

    // ----- Scenarios (optional overrides) -----
    /**
     * Named scenarios (e.g. light, heavy) to apply machine-type or user-specific overrides to connections.
     * Select via streamnova.pipeline.scenario=light or env STREAMNOVA_PIPELINE_SCENARIO.
     */
    private Map<String, ScenarioOverrides> scenarios;
    /** Default scenario when streamnova.pipeline.scenario is not set. */
    private String defaultScenario;

    // ----- Intermediate (staging): named handler-type configs -----
    /**
     * Named intermediate configs by handler type (e.g. gcs, bigquery).
     * YAML: pipeline.config.intermediates.gcs / .bigquery
     * When set, select via {@link #defaultIntermediate}; otherwise single {@link #intermediate} is used.
     */
    private Map<String, @Valid IntermediateConfig> intermediates;
    /** Default intermediate key for 3-stage pipelines (e.g. gcs, bigquery). */
    private String defaultIntermediate;

    /** Single staging config (legacy). YAML: pipeline.config.intermediate */
    @Valid
    private IntermediateConfig intermediate;

    // ----- Destination: named handler-type configs -----
    /**
     * Named destination configs by handler type (e.g. bigquery, gcs).
     * YAML: pipeline.config.destinations.bigquery / .gcs
     * When set, select via {@link #defaultDestination}; otherwise single {@link #target} is used.
     */
    private Map<String, @Valid TargetConfig> destinations;
    /** Default destination key (e.g. bigquery, gcs). */
    private String defaultDestination;

    /** Single target/sink config (legacy). YAML: pipeline.config.target */
    @Valid
    private TargetConfig target;

    // ----- Listener / pipeline-actions config (independent per 2-stage vs 3-stage) -----
    /**
     * Event listener / pipeline-actions config per stage mode.
     * Keys: "2-stage", "3-stage"; values: list of actions with optional eventType for logging.
     * YAML: pipeline.config.listeners.2-stage.actions / pipeline.config.listeners.3-stage.actions
     */
    private Map<String, @Valid StageActionsConfig> listeners;
}
