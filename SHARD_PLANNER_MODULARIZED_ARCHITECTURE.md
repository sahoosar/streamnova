# ShardPlanner: Modularized Architecture

## 🎯 Overview

The `ShardPlanner` has been **refactored and modularized** to provide a unified, machine-type-based calculation system. Shards and workers are now calculated **together as a cohesive unit** based on machine type resources.

---

## 📋 Key Changes

### **1. Unified Calculation Method**

**New Method:** `calculateOptimalShardWorkerPlan()`

This is the **PRIMARY method** that calculates both shards and workers together:

```java
ShardWorkerPlan plan = ShardPlanner.calculateOptimalShardWorkerPlan(
    pipelineOptions,
    databasePoolMaxSize,
    estimatedRowCount,
    averageRowSizeBytes,
    targetMbPerShard,
    userProvidedShardCount,    // From config (0 = auto)
    userProvidedWorkerCount,   // From config (0 = auto)
    userProvidedMachineType    // From config
);
```

**Returns:** `ShardWorkerPlan` containing:
- `shardCount` - Calculated or user-provided shard count
- `workerCount` - Calculated or user-provided worker count
- `machineType` - Machine type used for calculation
- `virtualCpus` - Virtual CPUs extracted from machine type
- `calculationStrategy` - Strategy used ("MACHINE_TYPE", "USER_PROVIDED", "RECORD_COUNT")

---

### **2. Required Fields in Config**

**File:** `pipeline_config.yml`

```yaml
pipeline:
  config:
    source:
      # Resource Configuration (REQUIRED - use 0 for auto-calculation)
      shards: 0      # REQUIRED - shard count (0 = calculate automatically based on machine type)
      workers: 0     # REQUIRED - worker count (0 = calculate automatically based on machine type)
      machineType:   # REQUIRED - machine type (e.g., "n2-standard-4", "n2-highcpu-8")
                     #           If not provided, will detect from PipelineOptions or use scenarios
```

**Behavior:**
- `shards: 0` → Calculate automatically based on machine type
- `workers: 0` → Calculate automatically based on machine type
- `machineType: ""` → Detect from PipelineOptions or use record-count scenarios

---

### **3. Unified Calculator Module**

**New Class:** `UnifiedCalculator`

Calculates both shards and workers together based on machine type:

```java
private static final class UnifiedCalculator {
    // Calculate optimal workers based on machine type
    static int calculateOptimalWorkersForMachineType(...)
    
    // Calculate optimal shards based on machine type
    static int calculateOptimalShardsForMachineType(...)
    
    // Calculate workers from shards (for record-count scenarios)
    static int calculateWorkersFromShards(...)
}
```

---

## 🏗️ Modularized Architecture

### **Module Organization:**

```
ShardPlanner
├── Configuration Constants
│   ├── DatasetScenarioConfig
│   ├── SizeBasedConfig
│   ├── CostOptimizationConfig
│   └── MachineProfile (record)
│
├── Public API
│   ├── calculateOptimalShardWorkerPlan() ← PRIMARY METHOD
│   ├── calculateOptimalShardCount() (legacy, for backward compatibility)
│   └── calculateOptimalWorkerCount()
│
├── Environment Detection
│   └── EnvironmentDetector
│       ├── detectEnvironment()
│       ├── detectMachineType()
│       ├── detectVirtualCpus()
│       └── detectWorkerCount()
│
├── Unified Calculation (NEW)
│   └── UnifiedCalculator
│       ├── calculateOptimalWorkersForMachineType()
│       ├── calculateOptimalShardsForMachineType()
│       └── calculateWorkersFromShards()
│
├── Machine Type Optimization
│   ├── MachineProfileProvider
│   ├── MachineTypeBasedOptimizer
│   ├── MachineTypeAdjuster
│   └── MachineTypeResourceValidator
│
├── Scenario Optimization
│   └── ScenarioOptimizer
│
├── Cost & Constraints
│   ├── CostOptimizer
│   ├── ConstraintApplier
│   └── ShardCountRounder
│
└── Helper Modules
    ├── DataSizeCalculator
    ├── SmallDatasetOptimizer
    ├── ProfileBasedCalculator
    └── WorkerCountCalculator
```

---

## 🔄 Calculation Flow

### **Unified Calculation Flow:**

```
┌─────────────────────────────────────┐
│  calculateOptimalShardWorkerPlan()  │
└──────────────┬──────────────────────┘
               │
               ▼
    ┌──────────────────────┐
    │ Detect Environment   │
    │ (machine type, vCPUs) │
    └──────────┬───────────┘
               │
    ┌──────────┴──────────┐
    │                     │
User Provides        User Provides
Both Shards &        Only One or
Workers?             Neither?
    │                     │
   YES                   NO
    │                     │
    ▼                     ▼
┌─────────────┐  ┌──────────────────┐
│ Validate    │  │ Machine Type      │
│ Against     │  │ Provided?         │
│ Machine Type│  └──────┬─────────────┘
└──────┬──────┘         │
       │         ┌──────┴───────┐
       │        YES             NO
       │         │               │
       │         ▼               ▼
       │   ┌──────────────┐  ┌──────────────┐
       │   │ Unified       │  │ Record-Count │
       │   │ Calculator    │  │ Scenarios    │
       │   │ (Machine     │  │              │
       │   │  Type Based) │  │              │
       │   └──────────────┘  └──────────────┘
       │         │               │
       └─────────┴───────────────┘
                 │
                 ▼
         ┌──────────────────┐
         │ Apply Constraints│
         │ & Rounding       │
         └──────────┬───────┘
                    │
                    ▼
         ┌──────────────────┐
         │ ShardWorkerPlan   │
         │ (shards, workers) │
         └──────────────────┘
```

---

## 📊 Priority Rules

### **1. User-Provided Values (Highest Priority)**
- If user provides both `shards` and `workers` → Validate against machine type and use
- If user provides only `shards` → Calculate workers based on machine type
- If user provides only `workers` → Calculate shards based on machine type and workers

### **2. Machine Type-Based Calculation (Primary)**
- If machine type provided (from config or PipelineOptions):
  - Calculate workers based on machine type and data size
  - Calculate shards based on machine type, workers, and data size
  - Use `UnifiedCalculator` for cohesive calculation

### **3. Record-Count Scenarios (Fallback)**
- If machine type NOT provided:
  - Calculate shards using record-count scenarios
  - Calculate workers from shards

---

## 🔧 Unified Calculator Logic

### **Machine Type-Based Calculation:**

```java
// Step 1: Calculate workers based on machine type
int workers = UnifiedCalculator.calculateOptimalWorkersForMachineType(
    environment, estimatedRowCount, averageRowSizeBytes, databasePoolMaxSize);

// Step 2: Update environment with calculated workers
environment = new ExecutionEnvironment(..., workers, ...);

// Step 3: Calculate shards based on machine type and workers
int shards = UnifiedCalculator.calculateOptimalShardsForMachineType(
    environment, estimatedRowCount, averageRowSizeBytes, 
    targetMbPerShard, databasePoolMaxSize);
```

### **Worker Calculation Formula:**

```java
// Based on machine type maximum
int maxWorkers = MachineTypeResourceValidator.calculateMaxWorkersForMachineType(environment, profile);

// Based on data size
int workersFromData = WorkerCountCalculator.calculateMinimumWorkersForData(...);

// Final: min(data requirements, machine type maximum)
int optimalWorkers = Math.min(workersFromData, maxWorkers);
optimalWorkers = WorkerCountCalculator.roundToOptimalWorkerCount(optimalWorkers);
```

### **Shard Calculation Formula:**

```java
// Use machine-type-based optimizer
int shards = MachineTypeBasedOptimizer.optimizeBasedOnMachineType(
    sizeBasedShardCount, estimatedRowCount, environment, profile, ...);

// Formula: workers × vCPUs × maxShardsPerVcpu
// High-CPU: maxShardsPerVcpu = 2
// Standard/High-Memory: maxShardsPerVcpu = 1
```

---

## 📝 Example Usage

### **Example 1: Auto-Calculation Based on Machine Type**

**Config:**
```yaml
pipeline:
  config:
    source:
      shards: 0        # Auto-calculate
      workers: 0        # Auto-calculate
      machineType: n2-highcpu-8  # User provides machine type
```

**Result:**
```
INFO: Machine type provided (n2-highcpu-8): calculating shards and workers based on machine type
INFO: Calculated optimal worker count: 16 workers (based on machine type: n2-highcpu-8)
INFO: Final plan [MACHINE_TYPE]: 128 shards, 16 workers (machine type: n2-highcpu-8, vCPUs: 8)
```

**Calculation:**
- Workers: Based on machine type max (8 vCPUs × 6 = 48 max, data requirements = 16) → **16 workers**
- Shards: Based on machine type (16 workers × 8 vCPUs × 2 maxShards/vCPU = 256 max, optimized for data) → **128 shards**

---

### **Example 2: User Provides Both Shards and Workers**

**Config:**
```yaml
pipeline:
  config:
    source:
      shards: 64       # User provides
      workers: 8       # User provides
      machineType: n2-standard-4
```

**Result:**
```
INFO: User-provided plan: 64 shards, 8 workers (machine type: n2-standard-4)
```

**Validation:**
- Max Shards: `8 workers × 4 vCPUs × 1 maxShards/vCPU = 32 shards`
- User provides: 64 shards
- **Adjusted:** 32 shards (capped at machine type maximum)

---

### **Example 3: User Provides Only Machine Type**

**Config:**
```yaml
pipeline:
  config:
    source:
      shards: 0        # Auto-calculate
      workers: 0        # Auto-calculate
      machineType: n2-highmem-4  # User provides machine type
```

**Result:**
```
INFO: Machine type provided (n2-highmem-4): calculating shards and workers based on machine type
INFO: Calculated optimal worker count: 8 workers (based on machine type: n2-highmem-4)
INFO: Final plan [MACHINE_TYPE]: 32 shards, 8 workers (machine type: n2-highmem-4, vCPUs: 4)
```

---

## 🎯 Benefits of Modularization

### **1. Unified Calculation:**
- ✅ Shards and workers calculated together
- ✅ Ensures consistency between shards and workers
- ✅ Machine type drives both calculations

### **2. Better Organization:**
- ✅ Clear module separation
- ✅ Single responsibility per module
- ✅ Easy to extend and maintain

### **3. Machine Type Priority:**
- ✅ Machine type is the primary factor
- ✅ All calculations respect machine type limits
- ✅ User values validated against machine type

### **4. Production Ready:**
- ✅ Comprehensive error handling
- ✅ Input validation
- ✅ Metrics collection
- ✅ Detailed logging

---

## 📋 Module Responsibilities

| Module | Responsibility |
|--------|---------------|
| **UnifiedCalculator** | Calculates shards and workers together based on machine type |
| **EnvironmentDetector** | Detects machine type, vCPUs, workers from PipelineOptions/config |
| **MachineTypeBasedOptimizer** | Optimizes shards based on machine type characteristics |
| **MachineTypeResourceValidator** | Validates user values against machine type limits |
| **ScenarioOptimizer** | Fallback optimization based on record count scenarios |
| **CostOptimizer** | Optimizes for cost efficiency |
| **ConstraintApplier** | Applies pool size, profile bounds constraints |
| **ShardCountRounder** | Rounds to optimal values (power-of-2 for GCP) |

---

## ✅ Summary

### **Key Improvements:**

1. ✅ **Unified Calculation Method** - `calculateOptimalShardWorkerPlan()` calculates both together
2. ✅ **Required Config Fields** - `shards` and `workers` are required (0 = auto-calculate)
3. ✅ **Machine Type Priority** - All calculations based on machine type when provided
4. ✅ **Modularized Architecture** - Clear separation of concerns
5. ✅ **UnifiedCalculator Module** - Dedicated module for machine-type-based calculations

### **Usage:**

```java
// In PostgresHandler
ShardWorkerPlan plan = ShardPlanner.calculateOptimalShardWorkerPlan(
    pipelineOptions, databasePoolMaxSize, estimatedRowCount, 
    averageRowSizeBytes, null, userShards, userWorkers, userMachineType);

int shardCount = plan.shardCount();
int workerCount = plan.workerCount();
```

**The ShardPlanner is now fully modularized and calculates shards and workers together based on machine type!**
