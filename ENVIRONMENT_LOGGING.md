# Environment Name in Log Messages

## 🎯 Overview

All log messages now include **environment name** (`[ENV: LOCAL]`, `[ENV: GCP]`, `[ENV: AWS]`) for easy tracking and debugging across different deployment environments.

---

## 📋 Log Format

### **Standard Format:**
```
[ENV: {ENVIRONMENT_NAME}] {log message}
```

**Examples:**
- `[ENV: LOCAL] Machine type not provided: automatically defaulting to LOCAL execution`
- `[ENV: GCP] Calculating optimal shards for machine type: n2-standard-4`
- `[ENV: AWS] AWS execution profile calculation: machineType=...`

---

## 🔍 Environment Names

| Environment | Log Prefix | Description |
|------------|------------|-------------|
| **LOCAL** | `[ENV: LOCAL]` | Local execution (development) |
| **GCP** | `[ENV: GCP]` | Google Cloud Platform (Dataflow) |
| **AWS** | `[ENV: AWS]` | Amazon Web Services (future) |
| **UNKNOWN** | `[ENV: UNKNOWN]` | Unknown/undetected environment |

---

## 📊 Log Message Examples

### **Environment Detection:**
```
[ENV: DETECTING] Starting generic environment detection (automatic configuration)...
[ENV: DETECTING] Cloud provider detected: LOCAL
[ENV: LOCAL] Machine type not provided: automatically defaulting to LOCAL execution (safe default for easy deployment)
[ENV: LOCAL] vCPUs for LOCAL execution: 8 (from available processors)
[ENV: LOCAL] Workers for LOCAL execution: 8 (based on 8 vCPUs)
[ENV: LOCAL] ═══════════════════════════════════════════════════════════════
[ENV: LOCAL] Environment Detection Summary
[ENV: LOCAL] ═══════════════════════════════════════════════════════════════
[ENV: LOCAL] Cloud Provider: LOCAL
[ENV: LOCAL] Machine Type: N/A (LOCAL execution)
[ENV: LOCAL] Virtual CPUs: 8
[ENV: LOCAL] Workers: 8
[ENV: LOCAL] Local Execution: true
[ENV: LOCAL] ═══════════════════════════════════════════════════════════════
[ENV: LOCAL] Deployment Mode: LOCAL - Optimized for local development
[ENV: LOCAL] Configuration: Minimal configuration required (auto-detected)
[ENV: LOCAL] Environment detection complete: LOCAL environment ready for deployment
```

### **Shard Calculation:**
```
[ENV: LOCAL] Size-based shard calculation: 1000.00 MB total / 200.0 MB per shard (target) = 5 shards
[ENV: LOCAL] Machine type not provided: applying scenario-based optimization based on record count
[ENV: LOCAL] Scenario-based optimization [MEDIUM (1M-5M)]: sizeBased=5 → optimized=80 shards
[ENV: LOCAL] Final plan [RECORD_COUNT]: 80 shards, 8 workers (machine type: local, vCPUs: 8)
```

### **GCP Deployment:**
```
[ENV: GCP] Machine type provided (n2-standard-4): calculating shards and workers based on machine type
[ENV: GCP] Calculated optimal worker count: 4 workers (based on machine type: n2-standard-4)
[ENV: GCP] Calculating optimal shards for machine type: n2-standard-4 (4 vCPUs, 4 workers)
[ENV: GCP] Data-size-based calculation: 1000.00 MB total → 5 shards (target: 200.0 MB per shard)
[ENV: GCP] Machine-type-based optimization starting [n2-standard-4]: 4 vCPUs, 4 workers, data-size-based=5, estimated-rows=500000
[ENV: GCP] Standard machine optimization: data-based=5, records-based=10, min-parallelism=2, max-from-cpu=16, final=10
[ENV: GCP] Final plan [MACHINE_TYPE]: 10 shards, 4 workers (machine type: n2-standard-4, vCPUs: 4)
```

### **Profile-Based Calculation:**
```
[ENV: LOCAL] Profile-based calculation starting: cloudProvider=LOCAL, machineType=null, vCPUs=8, workers=8
[ENV: LOCAL] Local execution profile calculation: vCPUs=8, workers=1
[ENV: LOCAL] Local profile calculation: baseShards=12 (vCPUs=8 × 1.5), poolCap=16, finalShards=12

[ENV: GCP] Profile-based calculation starting: cloudProvider=GCP, machineType=n2-standard-4, vCPUs=4, workers=4
[ENV: GCP] GCP Dataflow profile calculation: machineType=n2-standard-4, vCPUs=4, workers=4
[ENV: GCP] GCP profile calculation: queriesPerWorker=4, plannedConcurrency=16, safeConcurrency=16, shardsPerQuery=2.0, rawShardCount=32, finalShards=32
```

### **Scenario Optimization:**
```
[ENV: LOCAL] Scenario-based optimization [VERY SMALL (< 100K)]: sizeBased=5 → optimized=16 shards
[ENV: LOCAL] Very small dataset optimization (< 100K): 50000 records → 16 shards, ~3125 records/shard (target: 2K-5K) - maximum parallel processing

[ENV: LOCAL] Scenario-based optimization [MEDIUM (1M-5M)]: sizeBased=5 → optimized=80 shards
[ENV: LOCAL] Medium dataset optimization (1M-5M): 3000000 records → 80 shards, ~37500 records/shard (target: 25K-50K) - balanced cost/performance
```

---

## ✅ Benefits

### **1. Easy Environment Tracking**
- Quickly identify which environment logs are from
- Filter logs by environment name
- Track environment-specific issues

### **2. Better Debugging**
- Understand which calculation path was taken
- See environment-specific optimizations
- Track resource allocation per environment

### **3. Production Monitoring**
- Monitor GCP vs LOCAL deployments separately
- Track environment-specific performance
- Identify environment-specific issues

### **4. Log Filtering**
- Filter logs: `grep "[ENV: GCP]" logs.txt`
- Filter logs: `grep "[ENV: LOCAL]" logs.txt`
- Easy to separate local vs production logs

---

## 🔍 Log Message Categories

### **Environment Detection:**
- `[ENV: DETECTING]` - During environment detection
- `[ENV: {ENV}]` - After environment is determined

### **Shard Calculation:**
- `[ENV: {ENV}]` - All shard calculation messages
- Includes: size-based, row-count-based, scenario-based

### **Worker Calculation:**
- `[ENV: {ENV}]` - All worker calculation messages
- Includes: machine-type-based, data-based, cost-based

### **Profile-Based Calculation:**
- `[ENV: LOCAL]` - Local profile calculations
- `[ENV: GCP]` - GCP profile calculations
- `[ENV: AWS]` - AWS profile calculations (future)

### **Optimization:**
- `[ENV: {ENV}]` - Machine-type-based optimization
- `[ENV: {ENV}]` - Scenario-based optimization
- `[ENV: {ENV}]` - Cost optimization

---

## 📝 Example Log Output

### **Local Execution:**
```
[ENV: DETECTING] Starting generic environment detection (automatic configuration)...
[ENV: DETECTING] Cloud provider detected: LOCAL
[ENV: LOCAL] Machine type not provided: automatically defaulting to LOCAL execution (safe default for easy deployment)
[ENV: LOCAL] vCPUs for LOCAL execution: 8 (from available processors)
[ENV: LOCAL] Workers for LOCAL execution: 8 (based on 8 vCPUs)
[ENV: LOCAL] ═══════════════════════════════════════════════════════════════
[ENV: LOCAL] Environment Detection Summary
[ENV: LOCAL] ═══════════════════════════════════════════════════════════════
[ENV: LOCAL] Cloud Provider: LOCAL
[ENV: LOCAL] Machine Type: N/A (LOCAL execution)
[ENV: LOCAL] Virtual CPUs: 8
[ENV: LOCAL] Workers: 8
[ENV: LOCAL] Local Execution: true
[ENV: LOCAL] ═══════════════════════════════════════════════════════════════
[ENV: LOCAL] Machine type not provided: calculating shards and workers based on record count scenarios
[ENV: LOCAL] Size-based shard calculation: 1000.00 MB total / 200.0 MB per shard (target) = 5 shards
[ENV: LOCAL] Scenario-based optimization [MEDIUM (1M-5M)]: sizeBased=5 → optimized=80 shards
[ENV: LOCAL] Final plan [RECORD_COUNT]: 80 shards, 8 workers (machine type: local, vCPUs: 8)
```

### **GCP Deployment:**
```
[ENV: DETECTING] Starting generic environment detection (automatic configuration)...
[ENV: DETECTING] Cloud provider detected: GCP
[ENV: GCP] Machine type provided: n2-standard-4 (cloud provider: GCP)
[ENV: GCP] vCPUs from machine type n2-standard-4: 4
[ENV: GCP] Workers for GCP: will be calculated automatically by Dataflow
[ENV: GCP] ═══════════════════════════════════════════════════════════════
[ENV: GCP] Environment Detection Summary
[ENV: GCP] ═══════════════════════════════════════════════════════════════
[ENV: GCP] Cloud Provider: GCP
[ENV: GCP] Machine Type: n2-standard-4
[ENV: GCP] Virtual CPUs: 4
[ENV: GCP] Workers: 0
[ENV: GCP] Local Execution: false
[ENV: GCP] ═══════════════════════════════════════════════════════════════
[ENV: GCP] Machine type provided (n2-standard-4): calculating shards and workers based on machine type
[ENV: GCP] Calculating optimal shards for machine type: n2-standard-4 (4 vCPUs, 4 workers)
[ENV: GCP] Machine-type-based optimization starting [n2-standard-4]: 4 vCPUs, 4 workers, data-size-based=5, estimated-rows=500000
[ENV: GCP] Standard machine optimization: data-based=5, records-based=10, min-parallelism=2, max-from-cpu=16, final=10
[ENV: GCP] Final plan [MACHINE_TYPE]: 10 shards, 4 workers (machine type: n2-standard-4, vCPUs: 4)
```

---

## 🔧 Log Filtering Examples

### **Filter by Environment:**
```bash
# Show only LOCAL logs
grep "\[ENV: LOCAL\]" application.log

# Show only GCP logs
grep "\[ENV: GCP\]" application.log

# Show only AWS logs
grep "\[ENV: AWS\]" application.log

# Show all environment detection
grep "\[ENV: DETECTING\]" application.log
```

### **Filter by Operation:**
```bash
# Show shard calculations
grep "\[ENV:.*\] .*shard" application.log

# Show worker calculations
grep "\[ENV:.*\] .*worker" application.log

# Show optimization messages
grep "\[ENV:.*\] .*optimization" application.log
```

### **Filter by Environment and Operation:**
```bash
# Show GCP shard calculations
grep "\[ENV: GCP\].*shard" application.log

# Show LOCAL worker calculations
grep "\[ENV: LOCAL\].*worker" application.log
```

---

## 📋 Updated Classes

The following classes now include environment names in log messages:

1. **EnvironmentDetector** - Environment detection and configuration
2. **ShardPlanner** - Main shard and worker calculation
3. **UnifiedCalculator** - Unified shard/worker calculations
4. **MachineTypeBasedOptimizer** - Machine-type-based optimization
5. **ProfileBasedCalculator** - Profile-based calculations (LOCAL, GCP, AWS)
6. **ScenarioOptimizer** - Scenario-based optimization
7. **SmallDatasetOptimizer** - Small dataset optimization
8. **ShardCountRounder** - Shard count rounding

---

## ✅ Summary

**All log messages now include environment name for easy tracking:**

- ✅ Environment detection logs: `[ENV: DETECTING]`, `[ENV: LOCAL]`, `[ENV: GCP]`, `[ENV: AWS]`
- ✅ Shard calculation logs: `[ENV: {ENV}]`
- ✅ Worker calculation logs: `[ENV: {ENV}]`
- ✅ Optimization logs: `[ENV: {ENV}]`
- ✅ Error logs: `[ENV: {ENV}]`

**Benefits:**
- Easy to track which environment logs are from
- Simple log filtering by environment
- Better debugging and monitoring
- Clear separation of local vs production logs

**Environment names are now included in all log messages for easy tracking!** 🎯
