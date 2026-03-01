# Autonomous Batch Loading Agent

Orchestrator for a fully autonomous batch loading system: plan → optimize cost vs time → execute → learn.

**Design & flow diagrams:** See **DESIGN_AND_FLOW.md** (project root) for high-level design, end-to-end flow, recommend/execute sequences, config override, learning loop, and shard planning flow.

## Segments (one folder per segment)

| Segment | Package | Purpose |
|--------|---------|---------|
| **Profiler** | `agent.profiler` | Table profiling (row count, row size), Postgres warm-up throughput discovery |
| **Candidate Generator** | `agent.execution_planner` | Machine ladder (n2→n2d→c3), worker scaling, shard planning |
| **Estimator** | `agent.estimator` | Time & cost prediction per candidate (heuristic; USD internal) |
| **Recommender** | `agent.recommender` | COST_OPTIMAL / FAST_LOAD / BALANCED; picks best candidate by mode |
| **Execution Engine** | `agent.execution_engine` | ExecutionEngineService + **POST /api/agent/execute** (run with candidate; optional executionRunId, optional **source** for handler choice). **GET /api/agent/pipeline-handlers** for agentic handler discovery. See EXECUTION_ENGINE_STATUS.md. |
| **Metrics & Learning Store** | `agent.metrics` | Estimates vs actuals, throughput profiles, execution status; store + REST API |

## Profiler (implemented)

- **Row count estimate** – via existing `PostgresStatisticsEstimator` (pg_class or COUNT(*)).
- **Avg row size sampling** – from pg_class or sampled rows.
- **Postgres warm-up read** – small read to discover read throughput (MB/s).
- **ProfileStore** – interface + in-memory impl; use suggested schema for persistent learning store.

### Tables for tracking (Flyway)

**Flyway** runs migrations from `src/main/resources/db/migration/` when a DataSource is configured:

- **V1__agent_tables.sql** – `agent_table_profiles`, `agent_throughput_discovery`, `agent_runs`, `agent_estimates_vs_actuals` (PostgreSQL).
- **V2__dataflow_job_metadata.sql** – `dataflow_job_metadata` for Dataflow job status across request timeouts.

Configure **spring.datasource.url** (and username/password) to the PostgreSQL database where these tables should live; Flyway runs on startup and creates/updates schema. Same DB as pipeline source or a dedicated tracking DB both work. Reference SQL (for manual use) remains in `src/main/resources/agent/profiler/AGENT_TABLES_SCHEMA.sql` and `src/main/resources/agent/metrics/DATAFLOW_JOB_METADATA_SCHEMA.sql`.

## Candidate Generator (implemented)

- **Machine ladder** – `MachineLadder`: ordered GCP types n2 (standard, highmem, highcpu), n2d-standard, c3-standard (4, 8, 16/22 vCPUs). `getDefaultLadder()`, `getLadder(familyPrefix)`, `getLadderByVcpuTier(vCpus)`.
- **Worker scaling** – `WorkerScaling`: candidate worker counts (1, 2, 4, 8, 16, 32); `getReducedWorkerCandidates(max)` for bounded generation.
- **Shard planning** – Shard count for candidates comes from `agent.shardplanner.ShardPlanner.suggestShardCountForCandidate(machineType, workerCount, rowCount, rowSizeBytes, poolMaxSize)`, using the same table record size and max connection pool size logic as the pipeline. See **SHARD_PLANNER_ARCHITECTURE.md** and **SHARD_PLANNER_PRODUCTION_READINESS.md** (project root).
- **ExecutionPlanOption** – DTO: machineType, workerCount, shardCount, virtualCpus, suggestedPoolSize, label.
- **AdaptiveExecutionPlannerService** – `generate(TableProfile, profileRunId)` → `AdaptivePlanResult` (list of candidates); optional maxCandidates and machineFamily filter.
- **REST** – `GET /api/agent/candidates/generate` (profiles then generates); `GET /api/agent/candidates/generate-from-profile?runId=...` (from stored profile).

## Estimator (source / CPU / sink caps + time & cost)

- **LoadPattern** – `DIRECT` (source→BQ), `GCS_BQ` (source→GCS→BQ); used for sink cap.
- **Source cap** – `SourceCap`: Oracle cap 80 MB/s, Postgres 500 MB/s; effective source throughput = min(measured, cap).
- **CPU cap** – `CpuCap`: per-candidate cap = workers × vCPUs × 60 MB/s per vCPU.
- **Sink cap** – `SinkCap`: BQ direct 100 MB/s, GCS+BQ load 400 MB/s.
- **EstimationContext** – profile, loadPattern, sourceType, throughputSample; passed to `estimateWithCaps`.
- **Bottleneck** – SOURCE | CPU | SINK | PARALLELISM on each `EstimatedCandidate` (which cap limited throughput).
- **Time & cost** – effective throughput = min(parallel, sourceCap, cpuCap, sinkCap); duration = totalMb / effective; cost = duration × vCPU-hours × rate. When no warm-up throughput is available, **historical throughput** from getThroughputProfiles (same table) is used as fallback so cold runs get better estimates.

## Recommender (COST vs FAST scoring, Cheapest/Fastest/Balanced, Guardrails)

- **COST vs FAST scoring** – `RecommenderService.scoreCandidates(estimated, guardrails)` → list of `ScoredCandidate`: `costScore` and `fastScore` (0–100; higher = better). Best cost in list = 100 cost score; best duration = 100 fast score. `balancedScoreRaw` = cost×time (lower = better).
- **Cheapest / Fastest / Balanced** – `recommendCheapestFastestBalanced(estimated, guardrails, successCountByMachineType)` → `RecommendationTriple`: `cheapest`, `fastest`, `balanced` (each an `EstimatedCandidate`). When success counts are provided, ties are broken by historical success so all three picks prefer proven configs. Response includes all three plus `recommended` (for requested mode).
- **Guardrail enforcement** – `Guardrails`: optional `maxCostUsd`, `maxDurationSec`, `minThroughputMbPerSec`. `applyGuardrails(estimated, guardrails)` filters to passing candidates; `guardrailViolations(estimated, guardrails)` returns descriptions of failures. Recommendations are chosen only from candidates that pass.
- **API** – `GET /api/agent/recommend?mode=...&maxCostUsd=&maxDurationSec=&minThroughputMbPerSec=` returns `recommended`, `cheapest`, `fastest`, `balanced`, `scoredCandidates`, `guardrailsApplied`, `guardrailViolations`, `executionRunId`. **Default SLA**: when `streamnova.guardrails.max-duration-sec` and/or `streamnova.guardrails.max-cost-usd` are set, they apply when the client does not pass those params.

## Metrics & Learning Store (implemented)

- **Estimates vs actuals** – `EstimateVsActual`: runId, estimated/actual duration and cost, machineType, workerCount, shardCount. Recorded when execution finishes (POST execution-outcome). Queried for learning and reporting.
- **Throughput profiles** – `ThroughputProfile`: runId, sourceType, schema, table, bytesRead, durationMs, rowsRead, throughputMbPerSec. Recorded from Profiler warm-up when recommend API is used; can be saved explicitly via service.
- **Execution status** – `ExecutionStatus`: runId, profileRunId, mode, loadPattern, sourceType, schema, table, status (PLANNED/RUNNING/SUCCESS/FAILED), startedAt, finishedAt, jobId, message. Recorded when recommend returns (RUNNING); updated on POST execution-outcome.
- **MetricsLearningStore** – interface: saveEstimateVsActual, saveThroughputProfile, saveExecutionStatus, updateExecutionStatus; find* by runId or recent with optional table filter.
- **InMemoryMetricsLearningStore** – in-memory implementation (production: use JDBC with AGENT_TABLES_SCHEMA.sql).
- **MetricsLearningService** – recordRunStarted, recordRunFinished, recordEstimateVsActual, recordThroughputProfile; getEstimatesVsActuals, getThroughputProfiles, getRecentExecutionStatuses, getExecutionStatus; **getLearningSignals** (duration/cost correction by machine family, success count by machine type).
- **Learning loop** – After successful runs, POST execution-outcome records estimate vs actual. On each recommend, **EstimatorService** uses getLearningSignals to apply duration/cost correction factors per machine family (n2, n2d, c3) from past actuals so estimates improve over time. **RecommenderService** prefers candidates whose machine type has more successful runs (tie-break when mode score is equal). Thus after a few rounds the agent converges toward suggesting the candidate that is both predicted and historically appropriate.

## Agentic handler selection (no fixed handler per stage; 2 or 3 stages by user choice)

**No handler is fixed to source, intermediate, or target** — the user selects per stage. Examples:

- **2-stage**: source → destination (e.g. source=oracle, target=gcs; or source=postgres, target=bigquery).
- **3-stage**: source → intermediate → destination (e.g. source=oracle, intermediate=bigquery, target=gcs).

Config supports: **intermediate** handlerType gcs (gcsPath) or bigquery (dataset, table); **target** type bigquery (dataset, table) or gcs (gcsPath). Validation is type-driven only.

1. **Discover** – **GET /api/agent/pipeline-handlers** returns available handlers and current config mapping.

2. **Decide** – User chooses source, optional intermediate, and target (any supported type per stage).

3. **Execute** – **POST /api/agent/execute** body: optional **"source"**, **"intermediate"**, **"target"**. Omitted = use config. **PipelineHandlerResolver** resolves and validates by type; runner executes with resolved selection.

**Tracking** – All user-provided details and resolved execution are logged so the user can find what was run later: **\[HANDLER-SELECTION]** (user provided), **\[EXECUTION-TRACKING]** (resolved execution and 2-stage vs 3-stage).
