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

## Implemented: agent Execution Engine (simpler architecture)

- **`agent.execution_engine`** package: no component that
  1. Takes the **recommended** candidate (or a specific LoadCandidate) from the Recommender.
  2. Builds **PipelineOptions** (e.g. DataflowPipelineOptions: workerMachineType, maxNumWorkers, project, region) from that candidate.
  3. Creates the pipeline **with those options** and runs it (or submits to Dataflow).
- **No API** to trigger “run with recommended plan” or “run with this run ID”.
- **No link** from RecommendationResult → DataflowRunnerService with overrides.

So: **Dataflow job execution** (running a Beam pipeline, including on Dataflow) is **already implemented** in `DataflowRunnerService` and the handler/ShardPlanner. The **Execution Engine** in the agent sense (take recommendation → build options → run job) **needs to be implemented**.

---

## Recommended next steps

1. Add **ExecutionService** (or **ExecutionEngine**) in `agent.execution_engine` that:
   - Accepts a **LoadCandidate** (or runId to resolve the recommended candidate).
   - Builds **DataflowPipelineOptions** from it: `workerMachineType`, `maxNumWorkers`, project/region from config or env.
   - Calls a refactored **DataflowRunnerService.runPipeline(PipelineOptions, LoadCandidate overrides)** so the pipeline is created with those options and the handler uses the overridden shard count / machine type / workers for that run.
2. Optionally add **REST**: `POST /api/agent/execute` with body `{ "runId": "<recommendation run id>" }` or `{ "mode": "COST_OPTIMAL" }` to run the recommend flow and then execute with that recommendation.
3. Keep **StreamNovaApplication** as-is for “run once at startup” (config-only), or gate `runPipeline()` behind a profile so the agent API is the only trigger when using the agent.
