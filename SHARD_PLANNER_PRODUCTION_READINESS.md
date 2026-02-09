# ShardPlanner Production Readiness

## Overview

The ShardPlanner (`com.di.streamnova.agent.shardplanner`) is used by the agent to suggest shard count for each candidate (machine type + workers). It is **production-ready**: size-based and machine-type–based optimization, cost optimization, pool and profile constraints, and validation are implemented. See **SHARD_PLANNER_ARCHITECTURE.md** for the component flow.

---

## Calculation Flow

1. **Size-based** – When row count and row size are available: `totalSizeMb = rowCount × rowSizeBytes / (1024×1024)`; base shards = `ceil(totalSizeMb / targetMbPerShard)` (default 200 MB/shard).
2. **Machine-type optimization** – Adjusts by machine type (High-CPU → more shards per vCPU; High-Memory → balanced; Standard → based on total vCPUs and data minimums). Enforces profile max: `workers × vCPUs × maxShardsPerVcpu`.
3. **Cost optimization** – When shards would require > 1.5× available workers, reduces shards to fit worker count while respecting the size-based floor.
4. **Constraints** – Pool cap at 80% of `databasePoolMaxSize`; profile min/max; machine-type max.
5. **Rounding** – Rounded to a stable value for consistency.
6. **Final cap** – ShardPlanner enforces 1–256 and min-for-parallelism when workers > 1 but calculated shards would be 1.

---

## Features

- **Size-based calculation** when row size is known (target MB/shard configurable).
- **Machine-type tuning** for High-CPU, High-Memory, and Standard GCP types.
- **Cost optimization** to avoid over-provisioning (worker scaling threshold 1.5×).
- **Pool constraint** – shards capped at 80% of pool size when provided.
- **Profile bounds** – min/max shards per vCPU per machine type.
- **Input handling** – fallback when vCPUs unknown (e.g. 8 shards); null/zero inputs guarded.
- **Logging** – environment, machine type, size-based and final shard count.

---

## Configuration

- **Target MB per shard** – `SizeBasedConfig.DEFAULT_TARGET_MB_PER_SHARD` (200).
- **Pool** – `databasePoolMaxSize` passed from recommend config or request; 80% used as shard cap.
- **Cost** – `CostOptimizationConfig.WORKER_SCALING_THRESHOLD` (1.5×).
- **Max shards** – 256 (ShardPlanner).

---

## Production Checklist

- Size-based and machine-type–based calculation
- Cost optimization (worker-based)
- Pool and profile constraints
- Bounds and null-safe handling
- Logging for debugging and observability

---

## Deployment and Monitoring

- **Ready for production** – Used by AdaptiveExecutionPlannerService and PostgresHandler; candidate shards/workers/machineType/pool override pipeline config when executing with a recommendation.
- **Recommended monitoring** – Shard count per recommendation, pool-cap hits, planning errors if any.
