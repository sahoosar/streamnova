# ShardPlanner Architecture

Agent-only shard planning: suggests shard count for a candidate (machine type + workers) using table size and optional pool size. Used by `AdaptiveExecutionPlannerService` and `PostgresHandler`.

## Package

`com.di.streamnova.agent.shardplanner`

## Entry Point

- **ShardPlanner.suggestShardCountForCandidate(machineType, workerCount, estimatedRowCount, averageRowSizeBytes, databasePoolMaxSize)**  
  Returns suggested shard count (1–256). Uses `UnifiedCalculator` then applies min-for-parallelism when needed.

## Flow

1. **PoolSizeCalculator** – parses vCPUs from machine type (e.g. `n2-standard-4` → 4).
2. **ExecutionEnvironment** – built from machineType, vCPUs, workerCount, GCP.
3. **UnifiedCalculator.calculateOptimalShardsForMachineType**:
   - **DataSizeCalculator** – computes `DataSizeInfo` (totalSizeMb, hasSizeInformation) from row count and row size.
   - **Size-based** – when size is known: `shards = ceil(totalSizeMb / targetMbPerShard)` (default 200 MB/shard).
   - **MachineTypeBasedOptimizer** – adjusts by machine type:
     - **High-CPU** – more shards per vCPU (parallelism).
     - **High-Memory** – balanced, fewer shards per vCPU.
     - **Standard** – based on total vCPUs (workers × vCPUs) for minimum shards; applies data-size minimums.
   - **CostOptimizer** – reduces shards when workers needed > 1.5× available (cost awareness).
   - **ConstraintApplier** – pool cap (80% of databasePoolMaxSize), profile min/max, safety.
   - **ShardCountRounder** – rounds to a stable value (e.g. power-of-2–friendly).
4. **ShardPlanner** – if result is 1 but workerCount > 1, applies min-for-parallelism via `MachineTypeResourceValidator` and rounding; finally caps to 1–256.

## Components

| Class | Role |
|-------|------|
| **ShardPlanner** | Entry point; delegates to UnifiedCalculator, handles min-for-parallelism and final cap. |
| **UnifiedCalculator** | Orchestrates size-based calc, machine-type optimization, cost, constraints, rounding. |
| **DataSizeCalculator** | Row count + row size → DataSizeInfo (totalSizeMb, hasSizeInformation). |
| **DataSizeInfo** | DTO for total size (MB) and whether size is known. |
| **SizeBasedConfig** | Default target MB per shard (200). |
| **MachineTypeBasedOptimizer** | High-CPU / High-Memory / Standard shard calculation; enforces profile max and data minimums. |
| **MachineProfile** | Per–machine-type min/max shards per vCPU (from MachineProfileProvider). |
| **MachineProfileProvider** | Resolves MachineProfile for a machine type string. |
| **MachineTypeResourceValidator** | Max shards from profile (workers × vCPUs × maxShardsPerVcpu). |
| **CostOptimizer** | Reduces shards when over 1.5× workers needed vs available. |
| **CostOptimizationConfig** | Cost-optimization thresholds. |
| **ConstraintApplier** | Pool limit (80% of pool size), profile bounds. |
| **ShardCountRounder** | Rounds shard count to optimal value. |
| **PoolSizeCalculator** | Extracts vCPUs from machine type string. |
| **ExecutionEnvironment** | machineType, virtualCpus, workerCount, cloudProvider (GCP). |

## Configuration

- **Target MB per shard** – `SizeBasedConfig.DEFAULT_TARGET_MB_PER_SHARD` (200).
- **Pool cap** – shards capped at 80% of `databasePoolMaxSize` when provided.
- **Max shards** – hard cap 256 in ShardPlanner.

## References

- Candidate generation: `agent.adaptive_execution_planner.AdaptiveExecutionPlannerService` calls `ShardPlanner.suggestShardCountForCandidate(...)` with pool size from config or request.
- Pipeline config (connection, table) is separate; when executing with a candidate, shards/workers/machineType/pool come from the candidate (see DataflowRunnerService and pipeline config YAML comments).
