# Machine Type Requirement for GCP Dataflow Deployments

## 🎯 Overview

**Machine type is MANDATORY when deploying to GCP Dataflow.** The system now validates this requirement and will throw an `IllegalArgumentException` if machine type is missing for GCP deployments.

---

## ✅ Why Machine Type is Required for GCP

### **1. Resource Allocation**
- Machine type determines vCPUs, memory, and compute capacity
- Without machine type, the system cannot accurately calculate resource limits
- Leads to suboptimal shard and worker allocation

### **2. Cost Optimization**
- Different machine types have different costs
- System optimizes shards/workers based on machine type to balance cost and performance
- Without machine type, cost optimization cannot be applied

### **3. Resource Constraint Validation**
- Machine type defines maximum shards: `max_shards = workers × vCPUs × maxShardsPerVcpu`
- System validates user-provided shards/workers against machine type limits
- Prevents over-allocation that would fail at runtime

### **4. Production Readiness**
- Machine type is a critical production configuration
- Ensures predictable and reproducible deployments
- Enables proper monitoring and capacity planning

---

## 🔍 Validation Logic

### **Detection:**
```java
// System detects GCP Dataflow by checking:
DataflowPipelineOptions.getProject() != null && !blank
DataflowPipelineOptions.getRegion() != null && !blank
```

### **Validation:**
```java
IF GCP Dataflow detected:
    IF machine type is null or blank:
        → Throw IllegalArgumentException with clear error message
    ELSE:
        → Validation passed, proceed with calculation
ELSE (Local execution):
    → No validation required (machine type optional)
```

---

## 📋 How to Provide Machine Type

### **Option 1: Config File (Recommended)**

**File:** `src/main/resources/pipeline_config.yml`

```yaml
pipeline:
  config:
    source:
      type: postgres
      driver: org.postgresql.Driver
      jdbcUrl: jdbc:postgresql://...
      username: user
      password: pass
      table: my_table
      machineType: n2-standard-4  # ← REQUIRED for GCP
```

**Priority:** Highest (takes precedence over PipelineOptions)

---

### **Option 2: PipelineOptions**

```java
PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
DataflowPipelineOptions dataflowOptions = pipelineOptions.as(DataflowPipelineOptions.class);
dataflowOptions.setProject("my-project");
dataflowOptions.setRegion("us-central1");
dataflowOptions.setWorkerMachineType("n2-standard-4");  // ← REQUIRED for GCP
```

**Priority:** Medium (used if not provided in config)

---

### **Option 3: Command Line**

```bash
--machineType=n2-standard-4
```

**Priority:** Depends on how it's passed to PipelineOptions

---

## 🚫 Error Message

If machine type is missing for GCP deployment, you'll see:

```
❌ Machine type is REQUIRED for GCP Dataflow deployments.

GCP Dataflow detected (project and region set), but machine type is missing.

Please provide machine type in one of the following ways:
1. In pipeline_config.yml:
   pipeline:
     config:
       source:
         machineType: n2-standard-4

2. In PipelineOptions:
   dataflowOptions.setWorkerMachineType("n2-standard-4");

3. Via command line:
   --machineType=n2-standard-4

Machine type is mandatory for GCP to ensure:
- Proper resource allocation and cost optimization
- Accurate shard and worker calculations
- Resource constraint validation
- Production-ready configuration
```

---

## 📊 Supported Machine Types

### **Standard Machines:**
- `n2-standard-4` (4 vCPUs, balanced)
- `n2-standard-8` (8 vCPUs, balanced)
- `n2-standard-16` (16 vCPUs, balanced)
- `n2-standard-32` (32 vCPUs, balanced)

### **High-CPU Machines:**
- `n2-highcpu-8` (8 vCPUs, more CPU per core)
- `n2-highcpu-16` (16 vCPUs, more CPU per core)
- `n2-highcpu-32` (32 vCPUs, more CPU per core)

### **High-Memory Machines:**
- `n2-highmem-4` (4 vCPUs, memory-optimized)
- `n2-highmem-8` (8 vCPUs, memory-optimized)
- `n2-highmem-16` (16 vCPUs, memory-optimized)

---

## ✅ Local Execution (No Requirement)

**Machine type is NOT required for local execution:**

- System uses local CPU cores for vCPUs
- Falls back to record-count-based scenarios
- No validation error thrown

**Example:**
```java
// Local execution - machine type optional
PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
// No DataflowPipelineOptions set
// No machine type required ✅
```

---

## 🔄 Priority Order

When machine type is provided in multiple places:

```
1. Config File (pipeline_config.yml) ← HIGHEST PRIORITY
   ↓ (if not provided)
2. PipelineOptions (DataflowPipelineOptions.getWorkerMachineType())
   ↓ (if not provided)
3. null → ERROR for GCP, OK for local
```

---

## 📝 Examples

### **Example 1: GCP with Machine Type in Config ✅**

```yaml
# pipeline_config.yml
pipeline:
  config:
    source:
      machineType: n2-standard-4
```

**Result:**
```
✅ Machine type validation passed for GCP Dataflow: n2-standard-4 (provided via config file)
INFO: Machine type provided (n2-standard-4): calculating shards and workers based on machine type
```

---

### **Example 2: GCP with Machine Type in PipelineOptions ✅**

```java
DataflowPipelineOptions dataflowOptions = pipelineOptions.as(DataflowPipelineOptions.class);
dataflowOptions.setWorkerMachineType("n2-highcpu-8");
```

**Result:**
```
✅ Machine type validation passed for GCP Dataflow: n2-highcpu-8 (provided via PipelineOptions)
INFO: Machine type provided (n2-highcpu-8): calculating shards and workers based on machine type
```

---

### **Example 3: GCP WITHOUT Machine Type ❌**

```java
DataflowPipelineOptions dataflowOptions = pipelineOptions.as(DataflowPipelineOptions.class);
dataflowOptions.setProject("my-project");
dataflowOptions.setRegion("us-central1");
// Machine type NOT set
```

**Result:**
```
❌ IllegalArgumentException: Machine type is REQUIRED for GCP Dataflow deployments...
```

---

### **Example 4: Local Execution (No Machine Type) ✅**

```java
PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
// No DataflowPipelineOptions set
// No machine type required
```

**Result:**
```
DEBUG: Local execution detected - machine type validation skipped (not required for local)
INFO: Machine type not provided: calculating shards and workers based on record count scenarios
```

---

## 🎯 Summary

| Environment | Machine Type Required? | What Happens if Missing? |
|------------|----------------------|---------------------------|
| **GCP Dataflow** | ✅ **YES** | ❌ `IllegalArgumentException` thrown |
| **Local Execution** | ❌ **NO** | ✅ Falls back to record-count scenarios |

---

## 🔧 Implementation Details

**Validation Location:** `ShardPlanner.validateMachineTypeForGcpDeployment()`

**Called:** Early in `calculateOptimalShardWorkerPlan()`, before any calculations

**Detection Method:** `EnvironmentDetector.isGcpDataflow()`

**Error Type:** `IllegalArgumentException` with detailed error message

---

## ✅ Best Practices

1. **Always provide machine type in config file** for GCP deployments
2. **Use appropriate machine type** for your workload:
   - Standard: Balanced workloads
   - High-CPU: CPU-intensive workloads
   - High-Memory: Memory-intensive workloads
3. **Validate in CI/CD** to catch missing machine type before deployment
4. **Document machine type** in deployment documentation

---

**Machine type is now MANDATORY for GCP Dataflow deployments!** 🚀
