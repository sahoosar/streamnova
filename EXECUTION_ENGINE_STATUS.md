# Execution Engine – Dataflow Job Execution

## Already implemented (outside agent)

- **`DataflowRunnerService`** (runner package):
  - Creates an Apache Beam `Pipeline` with `Pipeline.create()`.
  - Loads config from YAML via `PipelineConfigService.getEffectiveLoadConfig()`.
  - Calls `SourceHandler.read(pipeline, config)` (e.g. PostgresHandler) which:
    - Uses **ShardPlanner** with `pipeline.getOptions()` for environment (Dataflow vs local), machine type, workers.
    - Reads from the source in parallel (shards).
  - Runs `pipeline.run()` and `result.waitUntilFinish()`; queries metrics.
- **ShardPlanner** and **EnvironmentDetector** already support **DataflowPipelineOptions** (worker machine type, maxNumWorkers, project, region). If the pipeline were created with those options, the job would run on GCP Dataflow with that configuration.
- **Entry point**: `StreamNovaApplication.main()` calls `DataflowRunnerService.runPipeline()` once at startup. No API trigger; no parameters from the Recommender.

So: **Beam pipeline execution and Dataflow-aware planning exist**, but the pipeline is created **without** Dataflow options (defaults to DirectRunner when run from main).

---

## Implemented: agent Execution Engine

- **`ExecutionEngineService`** takes an **ExecutionPlanOption** (candidate), enforces allowed-machine-types guardrail, and calls **DataflowRunnerService.runPipeline(candidate)** so the pipeline runs with that machine type, workers, and shards.
- **REST**: **POST /api/agent/execute** – body: `{ "candidate": { "machineType", "workerCount", "shardCount", "virtualCpus?", "suggestedPoolSize?", "label?" }, "executionRunId?" }`. Client can take `recommended.getCandidate()` from GET /api/agent/recommend and POST it here. When **executionRunId** is provided, execution status is updated to SUCCESS/FAILED after the run (fixes status staying RUNNING if client never POSTs execution-outcome).
- Flow: GET recommend → POST execute with recommended candidate → optionally POST execution-outcome with actual duration/cost for learning.

---

## Execution-time guardrail: allowed machine types

When `streamnova.guardrails.allowed-machine-types` is set in configuration (comma-separated, e.g. `n2,n2d`), **ExecutionEngineService** enforces it at execution time: `execute(ExecutionPlanOption candidate)` rejects the run if the candidate’s machine type is not in the allowed list (exact or family-prefix match, same rule as recommendation guardrails). This prevents callers from running a disallowed machine type even when bypassing the recommend API. Empty or unset = no execution-time restriction.
