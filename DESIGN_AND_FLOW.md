# StreamNova Agent – Design & Flow

High-level design and flow diagrams for the autonomous batch loading agent: **plan → recommend → execute → learn**.

---

## 1. High-Level Design

The agent orchestrates batch loads by:

1. **Plan** – Generate execution candidates (machine type, workers, shards) from table profile and shard planner.
2. **Recommend** – Estimate time/cost per candidate, apply guardrails, pick best by mode (COST_OPTIMAL / FAST_LOAD / BALANCED).
3. **Execute** – Run the pipeline with the chosen candidate; connection/table from config, execution plan from candidate.
4. **Learn** – Record estimates vs actuals and throughput; use learning signals to improve future estimates and prefer proven machine types.

```mermaid
flowchart LR
    subgraph Plan
        A[Table Profile] --> B[Candidates]
        B --> C[ShardPlanner]
    end
    subgraph Recommend
        C --> D[Estimator]
        D --> E[Guardrails]
        E --> F[Best Candidate]
    end
    subgraph Execute
        F --> G[DataflowRunner]
        G --> H[PostgresHandler]
    end
    subgraph Learn
        H --> I[Metrics Store]
        I --> J[Learning Signals]
        J --> D
    end
    Plan --> Recommend --> Execute --> Learn
```

---

## 2. Segment Overview

| Segment | Package | Purpose |
|--------|---------|---------|
| **Profiler** | `agent.profiler` | Table profile (row count, row size), warm-up throughput (MB/s) |
| **Candidate Generator** | `agent.execution_planner` | Machine ladder, worker scaling, ShardPlanner → list of candidates |
| **Estimator** | `agent.estimator` | Time & cost per candidate (source/CPU/sink caps, learning corrections) |
| **Recommender** | `agent.recommender` | Score candidates, apply guardrails, pick cheapest/fastest/balanced |
| **Execution Engine** | `agent.execution_engine` | POST /execute with candidate → DataflowRunnerService |
| **Metrics & Learning** | `agent.metrics` | Store estimates vs actuals, throughput, execution status; derive learning signals |

---

## 3. End-to-End API Flow

Typical user flow: **Recommend** → **Execute** → **Report outcome** (optional).

```mermaid
sequenceDiagram
    participant Client
    participant RecommendAPI
    participant ExecuteAPI
    participant MetricsAPI
    participant DataflowRunner
    participant PostgresHandler

    Client->>RecommendAPI: GET /api/agent/recommend?mode=...
    Note over RecommendAPI: Profile → Candidates → Estimate → Guardrails → Pick best
    RecommendAPI-->>Client: recommended, executionRunId, durationEstimate

    Client->>ExecuteAPI: POST /api/agent/execute { candidate, executionRunId? }
    ExecuteAPI->>DataflowRunner: runPipeline(candidate)
    Note over DataflowRunner: Override config: machineType, workers, shards, pool
    DataflowRunner->>PostgresHandler: read(pipeline, overriddenConfig)
    PostgresHandler-->>DataflowRunner: PCollection
    DataflowRunner-->>ExecuteAPI: jobId (async)
    ExecuteAPI-->>Client: 202 / jobId

    Note over Client: After job completes
    Client->>MetricsAPI: POST /api/agent/metrics/execution-outcome { runId, success, actualDurationSec, ... }
    MetricsAPI-->>Client: 200
```

---

## 4. Recommend Flow (Internal)

What happens inside **GET /api/agent/recommend**:

```mermaid
flowchart TB
    subgraph Input
        M[Mode: COST_OPTIMAL / FAST_LOAD / BALANCED]
        G[Guardrails: maxCost, maxDuration, minThroughput]
    end

    subgraph Profiling
        P1[Profile table: row count, row size]
        P2[Warm-up read → throughput MB/s]
    end

    subgraph CandidateGen["Candidate generation"]
        L[MachineLadder: n2, n2d, c3]
        W[WorkerScaling: 1, 2, 4, ...]
        S[ShardPlanner.suggestShardCountForCandidate]
        C[List of ExecutionPlanOption]
    end

    subgraph Estimate
        E[EstimatorService.estimateWithCaps]
        LS[LearningSignals: duration/cost correction, success count]
    end

    subgraph Recommend
        GR[Apply guardrails]
        SC[Score: costScore, fastScore]
        PK[Pick recommended + cheapest + fastest + balanced]
    end

    P1 --> P2
    P2 --> L
    L --> W
    W --> S
    S --> C
    C --> E
    LS --> E
    E --> GR
    G --> GR
    GR --> SC
    SC --> PK
    M --> PK
```

- **Profile** – Row count, row size, optional warm-up throughput.
- **Candidates** – Machine types × worker counts; shard count from `ShardPlanner.suggestShardCountForCandidate(machineType, workers, rowCount, rowSize, poolSize)`.
- **Estimate** – Per-candidate duration and cost with source/CPU/sink caps; learning signals correct by machine family and prefer successful machine types.
- **Recommend** – Filter by guardrails, score, return recommended + cheapest + fastest + balanced.

---

## 5. Execute Flow (Candidate → Pipeline)

When **POST /api/agent/execute** is called with a candidate:

```mermaid
flowchart LR
    subgraph Request
        A[Candidate: machineType, workers, shards, suggestedPoolSize]
    end

    subgraph Config
        B[Pipeline YAML: connection, table, fetchSize, ...]
    end

    subgraph DataflowRunnerService
        C[Load base config from YAML]
        D[Overwrite: machineType, workers, shards, pool]
        E[Build overriddenSource]
    end

    subgraph Run
        F[startLoadOperation pipeline, overriddenConfig]
        G[PostgresHandler.read config]
        H[Use config.getShards, config.getWorkers]
        I[pipeline.run]
    end

    A --> D
    B --> C
    C --> D
    D --> E
    E --> F
    F --> G
    G --> H
    H --> I
```

**Rule:** When a candidate is provided, **shards, workers, machineType, and pool** come **only from the candidate**. Pipeline config (YAML) is used only for **connection, table, and other read settings** (jdbcUrl, credentials, fetchSize, etc.).

---

## 6. Config Override (Candidate vs YAML)

```mermaid
flowchart TB
    subgraph PipelineConfig["Pipeline config (YAML)"]
        Y1[jdbcUrl, table, credentials]
        Y2[fetchSize, fetchFactor, ...]
        Y3[shards, workers, machineType, pool]
    end

    subgraph Candidate
        K1[machineType]
        K2[workerCount]
        K3[shardCount]
        K4[suggestedPoolSize]
    end

    subgraph EffectiveConfig["Effective config at runtime"]
        E1[From YAML: connection, table, etc.]
        E2[From candidate: machineType, workers, shards, pool]
    end

    Y1 --> E1
    Y2 --> E1
    Candidate --> E2
    note1[Y3 ignored when candidate provided]
    Y3 -.-> note1
```

So: **no reliance on pipeline config for shards, workers, machineType, or pool when the candidate supplies them**; we **still rely on pipeline config for connection, table, and all other read/source settings**.

---

## 7. Learning Loop

After runs complete, the agent uses outcomes to improve the next recommend.

```mermaid
flowchart TB
    subgraph Record
        A[POST execution-outcome: success, actualDurationSec, actualCostUsd]
        B[Save EstimateVsActual]
        C[Update ExecutionStatus SUCCESS/FAILED]
    end

    subgraph Signals
        D[MetricsLearningService.getLearningSignals]
        E[Duration correction by machine family]
        F[Cost correction by machine family]
        G[Success count by machine type]
    end

    subgraph NextRecommend
        H[Estimator: apply corrections to estimates]
        I[Recommender: prefer machine types with more successes]
    end

    A --> B
    A --> C
    B --> D
    D --> E
    D --> F
    D --> G
    E --> H
    F --> H
    G --> I
```

- **Estimator** – Applies duration/cost correction factors per machine family (n2, n2d, c3) from past actuals.
- **Recommender** – Uses success count by machine type to break ties and prefer historically successful configs.

---

## 8. Shard Planning Flow

How shard count is chosen for each candidate (used in candidate generation and documented in SHARD_PLANNER_ARCHITECTURE.md):

```mermaid
flowchart TB
    A[machineType, workerCount, rowCount, rowSize, poolMaxSize] --> B[PoolSizeCalculator: vCPUs]
    B --> C[ExecutionEnvironment]
    C --> D[DataSizeCalculator: totalSizeMb]
    D --> E[Size-based: ceil totalMb / 200]
    E --> F[MachineTypeBasedOptimizer: High-CPU / High-Mem / Standard]
    F --> G[CostOptimizer: reduce if workers needed >> available]
    G --> H[ConstraintApplier: pool 80%, profile min/max]
    H --> I[ShardCountRounder]
    I --> J[Cap 1–256; min-for-parallelism if workers>1]
    J --> K[Suggested shard count]
```

---

## 9. Key REST Endpoints

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/api/agent/recommend?mode=...` | Get recommended candidate + cheapest/fastest/balanced, executionRunId, durationEstimate |
| POST | `/api/agent/execute` | Run pipeline with candidate body; returns jobId (async) |
| POST | `/api/agent/metrics/execution-outcome` | Report run result (success, actualDurationSec, actualCostUsd) for learning |
| GET | `/api/agent/candidates/generate` | Generate candidates from profile (profile + generate) |
| GET | `/api/agent/metrics/table-statistics` | Chart-ready table statistics |

---

## 10. References

- **Agent design (segments, APIs)** – `src/main/java/com/di/streamnova/agent/README.md`
- **Execution engine** – `EXECUTION_ENGINE_STATUS.md`
- **Shard planner** – `SHARD_PLANNER_ARCHITECTURE.md`, `SHARD_PLANNER_PRODUCTION_READINESS.md`
- **Pipeline config** – event configs: `database_event_config.yml`, `gcs_event_config.yml`, `bq_event_config.yml`; or a single pipeline config file when not using event-configs-only (comment on candidate override)
