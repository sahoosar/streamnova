# Machine Type Priority Strategy

## 🎯 New Execution Strategy: Machine Type First

### **Priority Rule:**
1. **PRIMARY**: If machine type is provided → Use machine-type-based optimization
2. **FALLBACK**: If machine type is NOT provided → Use record-count-based scenarios

This rule is applied **throughout the program** - machine type takes precedence over record count scenarios.

---

## 📊 Execution Flow

### **Decision Tree:**

```
┌─────────────────────────────────────┐
│  Calculate Optimal Shard Count      │
└──────────────┬──────────────────────┘
               │
               ▼
    ┌──────────────────────┐
    │ Machine Type Provided?│
    └──────┬───────────────┘
           │
    ┌──────┴──────┐
    │             │
   YES           NO
    │             │
    ▼             ▼
┌─────────────────────┐  ┌──────────────────────────┐
│ MACHINE-TYPE-BASED   │  │ RECORD-COUNT-BASED        │
│ OPTIMIZATION         │  │ SCENARIOS (Fallback)      │
│ (PRIMARY)            │  │                          │
│                      │  │ - Very Small (< 100K)    │
│ - High-CPU           │  │ - Small-Medium (100K-500K)│
│ - High-Memory        │  │ - Medium-Small (500K-1M) │
│ - Standard           │  │ - Medium (1M-5M)        │
│                      │  │ - Large (5M-10M)         │
│ Based on:            │  │ - Very Large (> 10M)     │
│ - vCPUs              │  │                          │
│ - Workers            │  │ Based on record count   │
│ - Machine profile    │  │ thresholds              │
└─────────────────────┘  └──────────────────────────┘
```

---

## 🔧 Machine Type-Based Optimization (Primary)

### **When Machine Type is Provided:**

The system uses **machine-type characteristics** as the primary factor:

#### **1. High-CPU Machines** (e.g., `n2-highcpu-8`)
- **Strategy**: Maximum parallelism, more shards per vCPU
- **Calculation**: `workers × vCPUs × maxShardsPerVcpu` (aggressive)
- **Records per shard**: ~20,000 (smaller for better parallelism)
- **Example**: 8 workers × 8 vCPUs × 2 maxShards/vCPU = **128 shards base**

#### **2. High-Memory Machines** (e.g., `n2-highmem-4`)
- **Strategy**: Memory-optimized, fewer shards but larger per shard
- **Calculation**: `workers × vCPUs × maxShardsPerVcpu` (conservative)
- **Records per shard**: ~100,000 (larger for memory efficiency)
- **Example**: 4 workers × 4 vCPUs × 1 maxShards/vCPU = **16 shards base**

#### **3. Standard Machines** (e.g., `n2-standard-4`)
- **Strategy**: Balanced approach
- **Calculation**: `workers × vCPUs × maxShardsPerVcpu` (balanced)
- **Records per shard**: ~50,000 (balanced)
- **Example**: 4 workers × 4 vCPUs × 1 maxShards/vCPU = **16 shards base**

### **Machine Type Calculation Formula:**

```java
// Base calculation from machine type
int cpuBasedShards = workers × vCPUs × maxShardsPerVcpu

// Then adjusted based on:
// - Data size (if available)
// - Record count (if available)
// - Machine type characteristics (High-CPU vs High-Memory vs Standard)
```

---

## 📉 Record-Count-Based Scenarios (Fallback)

### **When Machine Type is NOT Provided:**

The system falls back to **record-count-based scenarios** (original behavior):

| Scenario | Record Range | Records/Shard | Strategy |
|----------|--------------|---------------|----------|
| Very Small | < 100K | 2K-5K | Maximum parallelism |
| Small-Medium | 100K-500K | 5K-10K | Fast parallel processing |
| Medium-Small | 500K-1M | 10K-20K | Balanced fast processing |
| Medium | 1M-5M | 25K-50K | Balanced cost/performance |
| Large | 5M-10M | 50K-100K | Cost-effective parallelism |
| Very Large | > 10M | 50K-100K | Scalable processing |

---

## 🔄 Implementation Details

### **Size-Based Strategy:**

```java
if (machineType != null && !machineType.isBlank() && !isLocalExecution) {
    // PRIMARY: Machine-type-based optimization
    shardCount = MachineTypeBasedOptimizer.optimizeBasedOnMachineType(...);
} else {
    // FALLBACK: Record-count-based scenarios
    shardCount = ScenarioOptimizer.optimizeForDatasetSize(...);
}
```

### **Row-Count-Based Strategy:**

```java
if (machineType != null && !machineType.isBlank() && !isLocalExecution) {
    // PRIMARY: Machine-type-based optimization
    shardCount = MachineTypeBasedOptimizer.optimizeBasedOnMachineType(...);
} else {
    // FALLBACK: Record-count-based scenarios
    shardCount = ScenarioOptimizer.optimizeForDatasetSize(...);
}
```

---

## 📝 Examples

### **Example 1: Machine Type Provided (High-CPU)**
```
Input:
- Machine: n2-highcpu-8 (8 vCPUs)
- Workers: 4
- Records: 6,000,000
- Row size: 200 bytes

Execution:
✅ Machine type provided → Use MACHINE-TYPE-BASED optimization
- High-CPU detected
- Base: 4 workers × 8 vCPUs × 2 maxShards/vCPU = 64 shards
- Adjusted for data: max(64, 6M / 20K) = max(64, 300) = 300 shards
- Final: 256 shards (power of 2, capped by constraints)

Result: 256 shards, ~23,400 records/shard
```

### **Example 2: Machine Type Provided (High-Memory)**
```
Input:
- Machine: n2-highmem-4 (4 vCPUs)
- Workers: 2
- Records: 6,000,000
- Row size: 200 bytes

Execution:
✅ Machine type provided → Use MACHINE-TYPE-BASED optimization
- High-Memory detected
- Base: 2 workers × 4 vCPUs × 1 maxShards/vCPU = 8 shards
- Adjusted for data: max(8, 6M / 100K) = max(8, 60) = 60 shards
- Final: 64 shards (power of 2)

Result: 64 shards, ~93,750 records/shard
```

### **Example 3: Machine Type NOT Provided**
```
Input:
- Machine: null (not provided)
- Workers: 8
- Records: 6,000,000
- Row size: 200 bytes

Execution:
❌ Machine type NOT provided → Use RECORD-COUNT-BASED scenarios
- 6M records → LARGE scenario (5M-10M)
- Records-based: 6M / 50K = 120 shards
- Final: 128 shards (power of 2)

Result: 128 shards, ~46,875 records/shard
```

---

## ✅ Key Benefits

### **1. Machine-Aware Optimization**
- **High-CPU**: Maximizes parallelism for CPU-bound workloads
- **High-Memory**: Optimizes for memory efficiency
- **Standard**: Balanced approach

### **2. Consistent Behavior**
- Machine type always takes precedence when provided
- Record-count scenarios only used as fallback
- No forcing of scenarios based on record count alone

### **3. Cost Efficiency**
- High-CPU: More shards = better CPU utilization
- High-Memory: Fewer shards = memory efficiency
- Standard: Balanced = cost-effective

### **4. Flexibility**
- Works with or without machine type
- Graceful fallback to record-count scenarios
- Handles all edge cases

---

## 🎯 Summary

### **New Priority Rule:**
1. **Machine Type Provided** → Machine-type-based optimization (PRIMARY)
2. **Machine Type NOT Provided** → Record-count-based scenarios (FALLBACK)

### **Applied Throughout:**
- ✅ Size-based calculation strategy
- ✅ Row-count-based calculation strategy
- ✅ All optimization paths
- ✅ Consistent behavior across all scenarios

### **Result:**
- **Machine type is the primary factor** when available
- **Record count scenarios are fallback** when machine type is not provided
- **No forcing of scenarios** - respects machine type first, then falls back to record count

---

## 📋 Verification

The code now:
- ✅ Checks machine type first in both calculation strategies
- ✅ Uses machine-type-based optimization when machine type is provided
- ✅ Falls back to record-count scenarios when machine type is not provided
- ✅ Applies this rule consistently throughout the program
- ✅ Does NOT force scenarios based on record count alone when machine type is available

**The execution now depends on machine type first, with record-count scenarios as fallback.**
