# User Control Options: Managing Shards and Workers

## 🎯 Overview

The `ShardPlanner` now supports **user-provided overrides** for both **shard count** and **worker count**. Users can specify these values in the configuration file, and the system will respect them instead of calculating automatically.

---

## 📋 Configuration Options

### **1. Shard Count Override**

Users can specify the exact number of shards to use:

```yaml
pipeline:
  config:
    source:
      shards: 64      # User-provided shard count (0 = calculate automatically)
```

**Behavior:**
- If `shards > 0`: Uses the specified shard count (with validation and constraints applied)
- If `shards = 0` or `null`: Calculates optimal shard count automatically

**Alternative:**
- `numPartitions` can also be used (legacy support):
```yaml
      numPartitions: 64  # Same as shards (for backward compatibility)
```

---

### **2. Worker Count Override**

Users can specify the exact number of workers to use:

```yaml
pipeline:
  config:
    source:
      workers: 8      # User-provided worker count (0 = calculate automatically)
```

**Behavior:**
- If `workers > 0`: Uses the specified worker count
- If `workers = 0` or `null`: Calculates optimal worker count automatically

---

## 🔄 Priority Order

### **Shard Count Priority:**

```
1. User-provided shard count (from config.shards or config.numPartitions)
   ↓ (if not provided)
2. Automatic calculation based on:
   - Machine type (if provided)
   - Data size (if available)
   - Record count scenarios (fallback)
```

### **Worker Count Priority:**

```
1. User-provided worker count (from config.workers)
   ↓ (if not provided)
2. Worker count from PipelineOptions (maxWorkers or numWorkers)
   ↓ (if not provided)
3. Automatic calculation based on:
   - Machine type
   - Data size
   - Shard count
```

---

## 📝 Examples

### **Example 1: User Provides Both Shards and Workers**

```yaml
pipeline:
  config:
    source:
      type: postgres
      table: large_table
      shards: 128      # User wants exactly 128 shards
      workers: 16      # User wants exactly 16 workers
```

**Result:**
- ✅ Uses **128 shards** (user-provided)
- ✅ Uses **16 workers** (user-provided)
- ✅ Constraints still applied (pool size, profile bounds)
- ✅ Rounding still applied (power-of-2 for GCP)

---

### **Example 2: User Provides Only Shards**

```yaml
pipeline:
  config:
    source:
      type: postgres
      table: medium_table
      shards: 64       # User wants exactly 64 shards
      workers: 0       # Calculate workers automatically
```

**Result:**
- ✅ Uses **64 shards** (user-provided)
- ✅ Calculates **optimal workers** based on:
  - Machine type
  - Data size
  - Shard count (64 shards / shards-per-worker)

---

### **Example 3: User Provides Only Workers**

```yaml
pipeline:
  config:
    source:
      type: postgres
      table: small_table
      shards: 0        # Calculate shards automatically
      workers: 4       # User wants exactly 4 workers
```

**Result:**
- ✅ Calculates **optimal shards** based on:
  - Machine type (if provided)
  - Data size
  - Record count scenarios
- ✅ Uses **4 workers** (user-provided)
- ✅ Shard calculation considers the 4 workers constraint

---

### **Example 4: Automatic Calculation (No User Overrides)**

```yaml
pipeline:
  config:
    source:
      type: postgres
      table: any_table
      shards: 0        # Calculate automatically
      workers: 0       # Calculate automatically
```

**Result:**
- ✅ Calculates **optimal shards** automatically
- ✅ Calculates **optimal workers** automatically
- ✅ Uses machine-type-based optimization (if machine type provided)
- ✅ Falls back to record-count scenarios (if machine type not provided)

---

## ⚙️ How It Works

### **1. Shard Count Override Logic:**

```java
// In PostgresHandler.calculateShardCount()
if (config.getShards() != null && config.getShards() > 0) {
    // Use user-provided shard count
    return validateShardCount(config.getShards());
} else if (config.getNumPartitions() > 0) {
    // Use numPartitions (legacy support)
    return validateShardCount(config.getNumPartitions());
} else {
    // Calculate automatically
    return ShardPlanner.calculateOptimalShardCount(...);
}
```

### **2. Worker Count Override Logic:**

```java
// In ShardPlanner.calculateOptimalShardCount()
if (userProvidedWorkerCount != null && userProvidedWorkerCount > 0) {
    // Use user-provided worker count
    environment = new ExecutionEnvironment(..., userProvidedWorkerCount, ...);
} else if (environment.workerCount > 0) {
    // Use worker count from PipelineOptions
    // (already detected)
} else {
    // Calculate optimal worker count
    int optimalWorkers = calculateOptimalWorkerCount(...);
}
```

### **3. User-Provided Shard Count Processing:**

```java
// In ShardPlanner.calculateOptimalShardCount()
if (userProvidedShardCount != null && userProvidedShardCount > 0) {
    // Validate user-provided shard count
    InputValidator.validateShardCount(userProvidedShardCount);
    
    // Apply constraints (pool size, profile bounds)
    shardCount = ConstraintApplier.applyConstraints(
            userProvidedShardCount, environment, profile, databasePoolMaxSize);
    
    // Apply rounding (power-of-2 for GCP)
    shardCount = ShardCountRounder.roundToOptimalValue(shardCount, environment);
    
    // Return user-provided shard count (with constraints applied)
    return shardCount;
}
```

---

## ✅ Validation & Constraints

### **Shard Count Validation:**
- ✅ Minimum: 1 shard
- ✅ Maximum: 10,000 shards (configurable)
- ✅ Pool size constraint: Capped at 80% of database pool capacity
- ✅ Profile bounds: Respects machine profile min/max shards
- ✅ Rounding: Power-of-2 for GCP, exact for local

### **Worker Count Validation:**
- ✅ Minimum: 1 worker
- ✅ Maximum: 1,000 workers
- ✅ Validation: Bounds checking before use

---

## 📊 Use Cases

### **Use Case 1: Performance Tuning**
**Scenario:** User knows their data and wants to optimize for specific performance characteristics.

```yaml
shards: 256      # High parallelism for fast processing
workers: 32      # Many workers for distributed processing
```

### **Use Case 2: Cost Optimization**
**Scenario:** User wants to limit costs by using fewer workers.

```yaml
shards: 64       # Moderate parallelism
workers: 4       # Limited workers to control costs
```

### **Use Case 3: Resource Constraints**
**Scenario:** User has specific resource limits (e.g., database connection pool).

```yaml
shards: 32       # Matches available database connections
workers: 8       # Matches available compute resources
```

### **Use Case 4: Testing & Development**
**Scenario:** User wants consistent behavior for testing.

```yaml
shards: 8        # Fixed shard count for reproducible tests
workers: 2       # Fixed worker count for local testing
```

---

## 🔍 Logging

The system logs when user-provided values are used:

```
INFO: Using user-provided shard count from config: 128 shards
INFO: Using user-provided worker count from config: 16 workers
INFO: User-provided shard count: 128 → 128 (after constraints and rounding)
```

Or when automatic calculation is used:

```
INFO: Machine type provided (n2-highcpu-8): using machine-type-based optimization
INFO: Calculated optimal worker count: 16 workers (based on machine type and data size)
```

---

## ⚠️ Important Notes

### **1. Machine Type Validation (HIGHEST PRIORITY):**
**Machine type resource limits are ALWAYS enforced**, even when users provide values:
- ✅ **Maximum Shards**: `workers × vCPUs × maxShardsPerVcpu`
  - High-CPU: `maxShardsPerVcpu = 2`
  - High-Memory/Standard: `maxShardsPerVcpu = 1`
- ✅ **Maximum Workers**: `vCPUs × multiplier`
  - High-CPU: `vCPUs × 6`
  - High-Memory: `vCPUs × 3`
  - Standard: `vCPUs × 4`
- ✅ **User values exceeding machine type limits are automatically capped**
- ✅ **Warnings are logged** when adjustments are made

**Example:**
- Machine: `n2-standard-4` (4 vCPUs)
- User provides: `shards: 256, workers: 4`
- Machine type max: `4 workers × 4 vCPUs × 1 = 16 shards`
- **Result:** `16 shards` (capped at machine type maximum)

### **2. Additional Constraints:**
After machine type validation, the system also applies:
- ✅ Database pool size constraints (80% headroom)
- ✅ Machine profile bounds (min/max shards per vCPU)
- ✅ Rounding (power-of-2 for GCP efficiency)
- ✅ Validation (bounds checking)

### **3. User Values May Be Adjusted:**
If user-provided values violate machine type or other constraints, they will be adjusted:
- Example 1: User provides 256 shards, but machine type max is 16 → Adjusted to 16 shards
- Example 2: User provides 64 workers, but machine type max is 16 → Adjusted to 16 workers
- Example 3: User provides 1000 shards, but pool size is 16 → Adjusted to 12 shards (80% of 16)

### **3. Backward Compatibility:**
- ✅ `numPartitions` still works (legacy support)
- ✅ `shards: 0` means "calculate automatically" (default behavior)
- ✅ `workers: 0` means "calculate automatically" (default behavior)

---

## 📋 Configuration Reference

### **Full Configuration Example:**

```yaml
pipeline:
  config:
    source:
      type: postgres
      driver: org.postgresql.Driver
      jdbcUrl: jdbc:postgresql://localhost:5432/mydb
      username: user
      password: pass
      table: my_table
      fetchSize: 5000
      maximumPoolSize: 16
      
      # User Control Options:
      shards: 64       # 0 = calculate automatically, > 0 = use this value
      workers: 8       # 0 = calculate automatically, > 0 = use this value
      partitions: 0    # Legacy: same as shards (0 = calculate automatically)
      fetchFactor: 1.0
```

---

## 🎯 Summary

### **What Users Can Control:**
- ✅ **Shard Count**: Specify exact number of shards (`shards` or `numPartitions`)
- ✅ **Worker Count**: Specify exact number of workers (`workers`)

### **What Still Happens Automatically:**
- ✅ **Constraint Application**: Pool size, profile bounds, rounding
- ✅ **Validation**: Bounds checking, security validation
- ✅ **Optimization**: When user values not provided

### **Priority Order:**
1. **User-provided values** (highest priority)
2. **PipelineOptions values** (medium priority)
3. **Automatic calculation** (lowest priority, fallback)

**Users now have full control over shards and workers while still benefiting from automatic optimization when not specified!**
