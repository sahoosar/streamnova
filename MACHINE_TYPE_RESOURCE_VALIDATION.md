# Machine Type Resource Validation

## 🎯 Overview

The system now **validates user-provided shard and worker counts against machine type resource limits**. Even when users specify shards/workers, the system ensures they don't exceed the machine type's capabilities.

---

## 🔒 Validation Rules

### **Priority Order:**

```
1. Machine Type Resource Limits (UPPER BOUND)
   ↓
2. User-Provided Values (if within machine type limits)
   ↓
3. Automatic Calculation (if user values exceed machine type limits)
```

**Key Principle:** Machine type always defines the **maximum** resources available. User-provided values are **capped** at machine type limits.

---

## 📊 Machine Type Resource Calculations

### **Maximum Shards Formula:**

```
max_shards = workers × vCPUs × maxShardsPerVcpu
```

**Machine Type Profiles:**
- **High-CPU**: `maxShardsPerVcpu = 2` (more parallelism)
- **High-Memory**: `maxShardsPerVcpu = 1` (memory-efficient)
- **Standard**: `maxShardsPerVcpu = 1` (balanced)

**Example:**
- Machine: `n2-highcpu-8` (8 vCPUs)
- Workers: 4
- Max Shards: `4 × 8 × 2 = 64 shards`

---

### **Maximum Workers Formula:**

```
max_workers = vCPUs × multiplier
```

**Machine Type Multipliers:**
- **High-CPU**: `vCPUs × 6` (can support more workers)
- **High-Memory**: `vCPUs × 3` (moderate workers)
- **Standard**: `vCPUs × 4` (balanced)

**Example:**
- Machine: `n2-standard-4` (4 vCPUs)
- Max Workers: `4 × 4 = 16 workers`

---

## 🔍 Validation Examples

### **Example 1: User Provides Shards Exceeding Machine Type**

**Configuration:**
```yaml
pipeline:
  config:
    source:
      shards: 256      # User wants 256 shards
      workers: 4
```

**Machine Type:** `n2-standard-4` (4 vCPUs, Standard profile)
- Max Shards: `4 workers × 4 vCPUs × 1 maxShards/vCPU = 16 shards`

**Result:**
```
WARN: User-provided shard count 256 exceeds machine type maximum 16 
      (4 workers × 4 vCPUs × 1 maxShards/vCPU). 
      Capping at machine type maximum.
INFO: User-provided shard count: 256 → 16 (after machine type validation, constraints, and rounding)
```

**Final:** `16 shards` (capped at machine type maximum)

---

### **Example 2: User Provides Workers Exceeding Machine Type**

**Configuration:**
```yaml
pipeline:
  config:
    source:
      workers: 64      # User wants 64 workers
```

**Machine Type:** `n2-standard-4` (4 vCPUs, Standard profile)
- Max Workers: `4 vCPUs × 4 = 16 workers`

**Result:**
```
WARN: User-provided worker count 64 adjusted to 16 based on machine type constraints 
      (4 vCPUs, max workers: 16)
INFO: Using user-provided worker count: 16 workers (within machine type limits)
```

**Final:** `16 workers` (capped at machine type maximum)

---

### **Example 3: User Provides Values Within Machine Type Limits**

**Configuration:**
```yaml
pipeline:
  config:
    source:
      shards: 32       # User wants 32 shards
      workers: 4
```

**Machine Type:** `n2-highcpu-8` (8 vCPUs, High-CPU profile)
- Max Shards: `4 workers × 8 vCPUs × 2 maxShards/vCPU = 64 shards`

**Result:**
```
INFO: User-provided shard count 32 is within machine type limits (max: 64)
INFO: Using user-provided worker count: 4 workers (within machine type limits)
INFO: User-provided shard count: 32 → 32 (after machine type validation, constraints, and rounding)
```

**Final:** `32 shards, 4 workers` (user values accepted, within limits)

---

### **Example 4: High-CPU Machine with More Resources**

**Configuration:**
```yaml
pipeline:
  config:
    source:
      shards: 128      # User wants 128 shards
      workers: 8
```

**Machine Type:** `n2-highcpu-8` (8 vCPUs, High-CPU profile)
- Max Shards: `8 workers × 8 vCPUs × 2 maxShards/vCPU = 128 shards`

**Result:**
```
INFO: User-provided shard count 128 is within machine type limits (max: 128)
INFO: Using user-provided worker count: 8 workers (within machine type limits)
INFO: User-provided shard count: 128 → 128 (after machine type validation, constraints, and rounding)
```

**Final:** `128 shards, 8 workers` (user values accepted, exactly at maximum)

---

## 🔧 Implementation Details

### **1. Shard Count Validation:**

```java
// Calculate machine type maximum
int maxShardsFromMachineType = workers × vCPUs × maxShardsPerVcpu

// Validate user-provided shard count
if (userProvidedShardCount > maxShardsFromMachineType) {
    // Cap at machine type maximum
    shardCount = maxShardsFromMachineType
    log.warn("User-provided shard count {} exceeds machine type maximum {}", 
             userProvidedShardCount, maxShardsFromMachineType);
} else {
    // Use user-provided value
    shardCount = userProvidedShardCount
    log.info("User-provided shard count {} is within machine type limits", 
             userProvidedShardCount);
}
```

### **2. Worker Count Validation:**

```java
// Calculate machine type maximum
int maxWorkers = vCPUs × multiplier
// High-CPU: × 6, High-Memory: × 3, Standard: × 4

// Validate user-provided worker count
if (userProvidedWorkerCount > maxWorkers) {
    // Cap at machine type maximum
    workerCount = maxWorkers
    log.warn("User-provided worker count {} adjusted to {} based on machine type constraints", 
             userProvidedWorkerCount, maxWorkers);
} else {
    // Use user-provided value
    workerCount = userProvidedWorkerCount
    log.info("Using user-provided worker count: {} workers (within machine type limits)", 
             workerCount);
}
```

---

## 📋 Machine Type Resource Limits

### **High-CPU Machines** (e.g., `n2-highcpu-8`):

| vCPUs | Max Workers | Max Shards (4 workers) | Max Shards (8 workers) |
|-------|-------------|------------------------|------------------------|
| 4     | 24          | 32                     | 64                     |
| 8     | 48          | 64                     | 128                    |
| 16    | 96          | 128                    | 256                    |

**Formula:**
- Max Workers: `vCPUs × 6`
- Max Shards: `workers × vCPUs × 2`

---

### **High-Memory Machines** (e.g., `n2-highmem-4`):

| vCPUs | Max Workers | Max Shards (4 workers) | Max Shards (8 workers) |
|-------|-------------|------------------------|------------------------|
| 4     | 12          | 16                     | 32                     |
| 8     | 24          | 32                     | 64                     |
| 16    | 48          | 64                     | 128                    |

**Formula:**
- Max Workers: `vCPUs × 3`
- Max Shards: `workers × vCPUs × 1`

---

### **Standard Machines** (e.g., `n2-standard-4`):

| vCPUs | Max Workers | Max Shards (4 workers) | Max Shards (8 workers) |
|-------|-------------|------------------------|------------------------|
| 4     | 16          | 16                     | 32                     |
| 8     | 32          | 32                     | 64                     |
| 16    | 64          | 64                     | 128                    |

**Formula:**
- Max Workers: `vCPUs × 4`
- Max Shards: `workers × vCPUs × 1`

---

## ⚠️ Important Notes

### **1. Machine Type Always Wins:**
- User-provided values **cannot exceed** machine type maximums
- System **automatically caps** user values at machine type limits
- **Warnings are logged** when adjustments are made

### **2. Validation Happens First:**
- Machine type validation occurs **before** other constraints
- Pool size and other constraints are applied **after** machine type validation
- This ensures resources are allocated according to machine capabilities

### **3. Local Execution:**
- For local execution, machine type validation is **less strict**
- Uses available CPU cores as the basis for limits
- Worker concept doesn't apply the same way

### **4. No Machine Type:**
- If machine type is **not provided**, system uses **default limits**
- Default max workers: 100
- Default max shards: 1000
- **Warning is logged** when defaults are used

---

## 📊 Summary

### **Validation Flow:**

```
User Provides Shards/Workers
         ↓
Machine Type Detected?
    Yes → Calculate Machine Type Maximums
    No  → Use Default Limits
         ↓
User Values > Machine Type Maximums?
    Yes → Cap at Machine Type Maximums (WARN)
    No  → Use User Values (INFO)
         ↓
Apply Additional Constraints (pool size, etc.)
         ↓
Final Shard/Worker Count
```

### **Key Benefits:**
- ✅ **Prevents over-allocation** of resources
- ✅ **Respects machine type capabilities**
- ✅ **User values still honored** (when within limits)
- ✅ **Automatic adjustment** when user values exceed limits
- ✅ **Clear logging** of adjustments

**The system now ensures resources are allocated according to machine type, even when users provide specific values!**
