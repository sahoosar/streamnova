# Shard Calculation When NO Machine Type is Provided

## 🎯 Overview

When **machine type is NOT provided** (neither from config nor PipelineOptions), the system uses **data-driven calculation** based on:
1. **Data Size** (MB) - if available
2. **Record Count** - always considered
3. **Record Count Scenarios** - optimized strategies for different dataset sizes
4. **Local CPU Cores** - as a constraint (not a target)
5. **Database Pool Limits** - as a constraint

---

## 📊 Decision Flow

### **Step 1: Check Data Size Information**

```
IF data size information (MB) is available:
    → Use calculateShardsUsingSizeBasedStrategy()
ELSE:
    → Use calculateShardsUsingRowCountBasedStrategy()
```

---

## 🔄 Path 1: Size-Based Strategy (Data Size Available)

### **Calculation Steps:**

1. **Base Calculation:**
   ```
   shards = ceil(total_size_mb / target_mb_per_shard)
   Default: target_mb_per_shard = 200 MB
   ```

2. **Scenario Optimization:**
   Applies record-count-based scenarios to refine the shard count:

   | Dataset Size | Records/Shard Target | Min Shards | Max Shards | Strategy |
   |-------------|----------------------|------------|------------|----------|
   | < 100K | 2K-5K | 16 | Unlimited | Maximum parallelism |
   | 100K-500K | 5K-10K | 12 | Unlimited | Fast parallel processing |
   | 500K-1M | 10K-20K | 10 | 100 | Balanced fast processing |
   | 1M-5M | 25K-50K | - | 200 | Balanced cost/performance |
   | 5M-10M | 50K-100K | - | 500 | Cost-effective with good parallelism |
   | > 10M | 50K-100K | - | 500 | Cost-effective with good parallelism |

3. **Cost Optimization:**
   - Optimizes for cost efficiency
   - Considers worker limits

4. **Constraints:**
   - Database pool limits (80% headroom)
   - Local CPU cores (as maximum, not target)

5. **Rounding:**
   - Power-of-2 for GCP
   - Exact for local execution

### **Example:**

**Input:**
- Total size: 1,000 MB
- Record count: 500,000 records
- No machine type

**Calculation:**
1. Base: `ceil(1000 MB / 200 MB) = 5 shards`
2. Scenario: Small-Medium (100K-500K) → `ceil(500,000 / 5,000) = 100 shards`
3. Take maximum: `max(5, 100) = 100 shards`
4. Apply constraints: Capped by database pool if needed
5. Final: **100 shards**

---

## 🔄 Path 2: Row-Count-Based Strategy (Data Size NOT Available)

### **Calculation Steps:**

1. **Very Small Datasets (< 10K records):**
   ```
   Uses SmallDatasetOptimizer:
   - shards = min(total_cores, max_shards_from_data)
   - Target: 50+ records per shard
   - Respects database pool limits
   ```

2. **Larger Datasets (>= 10K records):**
   
   a. **Base Calculation (ProfileBasedCalculator):**
   ```
   queriesPerWorker = ceil(vCPUs × jdbcConnectionsPerVcpu)
   plannedConcurrency = workers × queriesPerWorker
   safeConcurrency = min(plannedConcurrency, pool_capacity × 0.8)
   shards = safeConcurrency × shardsPerQuery
   ```
   
   b. **Scenario Optimization:**
   - Same scenarios as Path 1 (based on record count)
   - Refines the profile-based calculation
   
   c. **Constraints:**
   - Database pool limits
   - Local CPU cores

### **Example:**

**Input:**
- Record count: 2,000,000 records
- No data size info
- Local execution: 8 vCPUs, 1 worker
- No machine type

**Calculation:**
1. Profile-based base:
   - `queriesPerWorker = ceil(8 × 1.0) = 8`
   - `plannedConcurrency = 1 × 8 = 8`
   - `shards = 8 × 2.0 = 16 shards` (base)

2. Scenario: Medium (1M-5M) → `ceil(2,000,000 / 25,000) = 80 shards`
3. Take maximum: `max(16, 80) = 80 shards`
4. Apply constraints: Capped at 200 (MAX_SHARDS_MEDIUM)
5. Final: **80 shards**

---

## 👥 Worker Calculation (from Shards)

When machine type is NOT provided, workers are calculated from shards:

```java
UnifiedCalculator.calculateWorkersFromShards(shardCount, environment)
```

**Formula:**
```
shardsPerWorker = calculateOptimalShardsPerWorker(environment)
workers = ceil(shards / shardsPerWorker)
workers = roundToOptimalWorkerCount(workers)  // Power-of-2 for GCP
```

**Example:**
- Shards: 100
- Shards per worker: 4
- Workers: `ceil(100 / 4) = 25` → rounded to **32** (power-of-2)

---

## 📋 Complete Flow Diagram

```
NO Machine Type Provided
    ↓
Check Data Size Info
    ↓
    ├─→ [Data Size Available]
    │       ↓
    │   calculateShardsUsingSizeBasedStrategy()
    │       ↓
    │   STEP 1: Base = total_size_mb / 200 MB
    │       ↓
    │   STEP 2: ScenarioOptimizer.optimizeForDatasetSize()
    │       ↓
    │   STEP 3: CostOptimizer.optimizeForCost()
    │       ↓
    │   STEP 4: ConstraintApplier.applyConstraints()
    │       ↓
    │   STEP 5: ShardCountRounder.roundToOptimalValue()
    │       ↓
    │   FINAL SHARD COUNT
    │
    └─→ [Data Size NOT Available]
            ↓
        calculateShardsUsingRowCountBasedStrategy()
            ↓
        IF records < 10K:
            SmallDatasetOptimizer.calculateForSmallDataset()
        ELSE:
            STEP 1: ProfileBasedCalculator.calculateUsingProfile()
                ↓
            STEP 2: ScenarioOptimizer.optimizeForDatasetSize()
                ↓
            STEP 3: CostOptimizer.optimizeForCost()
                ↓
            STEP 4: ConstraintApplier.applyConstraints()
                ↓
            STEP 5: ShardCountRounder.roundToOptimalValue()
                ↓
            FINAL SHARD COUNT
```

---

## 🔑 Key Principles

1. **Data-Driven:** Shards are calculated based on data size and record count, not forced to match vCPUs
2. **Scenario-Based:** Different optimization strategies for different dataset sizes
3. **Cost-Effective:** Considers cost efficiency, especially for larger datasets
4. **Constrained:** Respects database pool limits and local CPU cores (as maximum)
5. **Flexible:** No machine type constraints, allowing optimal shard count for data

---

## 📊 Scenario Details

### **Very Small (< 100K records):**
- **Target:** 2K-5K records/shard
- **Min Shards:** 16
- **Strategy:** Maximum parallelism
- **Example:** 50K records → 10-25 shards

### **Small-Medium (100K-500K):**
- **Target:** 5K-10K records/shard
- **Min Shards:** 12
- **Strategy:** Fast parallel processing
- **Example:** 300K records → 30-60 shards

### **Medium-Small (500K-1M):**
- **Target:** 10K-20K records/shard
- **Min Shards:** 10
- **Max Shards:** 100
- **Strategy:** Balanced fast processing
- **Example:** 800K records → 40-80 shards

### **Medium (1M-5M):**
- **Target:** 25K-50K records/shard
- **Max Shards:** 200
- **Strategy:** Balanced cost/performance
- **Example:** 3M records → 60-120 shards

### **Large (5M-10M):**
- **Target:** 50K-100K records/shard
- **Max Shards:** 500
- **Strategy:** Cost-effective with good parallelism
- **Example:** 8M records → 80-160 shards

---

## ✅ Summary

**When NO machine type is provided:**

1. **Shards are calculated from:**
   - Data size (if available) → `total_size_mb / 200 MB`
   - Record count scenarios → Optimized targets per dataset size
   - Profile-based calculation (if no size info) → `vCPUs × workers × profile factors`

2. **Workers are calculated from:**
   - Shard count → `ceil(shards / shards_per_worker)`
   - Rounded to power-of-2 for GCP efficiency

3. **Constraints applied:**
   - Database pool limits (80% headroom)
   - Local CPU cores (as maximum, not target)
   - Scenario-specific min/max limits

4. **No machine type constraints:**
   - Shards are NOT forced to match vCPUs
   - Calculation is purely data-driven
   - Optimal for the actual data size and record count
