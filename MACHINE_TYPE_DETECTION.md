# How ShardPlanner Reads Machine Type

## 🎯 Overview

The `ShardPlanner` detects machine type from **Apache Beam PipelineOptions**, specifically from the `DataflowPipelineOptions` interface which provides GCP Dataflow execution environment information.

---

## 📊 Machine Type Detection Flow

```
PipelineOptions
      ↓
DataflowPipelineOptions.getWorkerMachineType()
      ↓
Machine Type String (e.g., "n2-standard-4")
      ↓
EnvironmentDetector.detectMachineType()
      ↓
ExecutionEnvironment.machineType
      ↓
Used for optimization and validation
```

---

## 🔍 Detection Process

### **Step 1: Access PipelineOptions**

The `ShardPlanner` receives `PipelineOptions` as a parameter:

```java
public static int calculateOptimalShardCount(
    PipelineOptions pipelineOptions,  // ← Machine type source
    Integer databasePoolMaxSize,
    Long estimatedRowCount,
    Integer averageRowSizeBytes,
    Double targetMbPerShard,
    Integer userProvidedShardCount,
    Integer userProvidedWorkerCount)
```

---

### **Step 2: Detect Environment**

The `EnvironmentDetector` class extracts machine type from `PipelineOptions`:

```java
private static final class EnvironmentDetector {
    static ExecutionEnvironment detectEnvironment(PipelineOptions pipelineOptions) {
        String machineType = detectMachineType(pipelineOptions);  // ← Machine type detection
        int virtualCpus = detectVirtualCpus(pipelineOptions);
        int workerCount = detectWorkerCount(pipelineOptions);
        return new ExecutionEnvironment(machineType, virtualCpus, workerCount);
    }
}
```

---

### **Step 3: Extract Machine Type**

The `detectMachineType()` method reads from `DataflowPipelineOptions`:

```java
private static String detectMachineType(PipelineOptions pipelineOptions) {
    try {
        // Cast PipelineOptions to DataflowPipelineOptions
        DataflowPipelineOptions dataflowOptions = pipelineOptions.as(DataflowPipelineOptions.class);
        
        // Get machine type from DataflowPipelineOptions
        return dataflowOptions.getWorkerMachineType();  // ← Returns machine type string
    } catch (Exception e) {
        log.debug("Failed to detect machine type from PipelineOptions: {}", e.getMessage());
        return null;  // ← Returns null if not available (local execution)
    }
}
```

---

## 📋 Code Location

**File:** `src/main/java/com/di/streamnova/util/ShardPlanner.java`

**Method:** `EnvironmentDetector.detectMachineType()` (Lines 365-372)

```java
private static String detectMachineType(PipelineOptions pipelineOptions) {
    try {
        return pipelineOptions.as(DataflowPipelineOptions.class).getWorkerMachineType();
    } catch (Exception e) {
        log.debug("Failed to detect machine type from PipelineOptions: {}", e.getMessage());
        return null;
    }
}
```

---

## 🔧 How It Works

### **1. PipelineOptions Interface**

`PipelineOptions` is an Apache Beam interface that provides access to pipeline configuration. For GCP Dataflow, it extends to `DataflowPipelineOptions`.

### **2. DataflowPipelineOptions**

`DataflowPipelineOptions` is a GCP-specific extension that includes:
- `getWorkerMachineType()` - Returns machine type string (e.g., "n2-standard-4")
- `getMaxNumWorkers()` - Returns maximum number of workers
- `getNumWorkers()` - Returns number of workers
- Other GCP-specific options

### **3. Machine Type String Format**

GCP machine types follow this format:
- **Standard**: `n2-standard-4` (family-series-size)
- **High-CPU**: `n2-highcpu-8` (family-series-size)
- **High-Memory**: `n2-highmem-4` (family-series-size)

**Examples:**
- `n2-standard-4` → 4 vCPUs, standard machine
- `n2-highcpu-8` → 8 vCPUs, high-CPU machine
- `n2-highmem-16` → 16 vCPUs, high-memory machine

---

## 📊 Detection Scenarios

### **Scenario 1: GCP Dataflow Execution**

**When:** Pipeline runs on GCP Dataflow

**Process:**
```java
PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
DataflowPipelineOptions dataflowOptions = pipelineOptions.as(DataflowPipelineOptions.class);
dataflowOptions.setWorkerMachineType("n2-standard-4");  // ← Set machine type
dataflowOptions.setProject("my-project");
dataflowOptions.setRegion("us-central1");

// ShardPlanner detects it:
String machineType = dataflowOptions.getWorkerMachineType();  // Returns "n2-standard-4"
```

**Result:**
- ✅ Machine type detected: `"n2-standard-4"`
- ✅ vCPUs extracted: `4` (from last part of machine type)
- ✅ Uses machine-type-based optimization

---

### **Scenario 2: Local Execution**

**When:** Pipeline runs locally (not on GCP)

**Process:**
```java
PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
// No DataflowPipelineOptions set (local execution)

// ShardPlanner tries to detect:
try {
    DataflowPipelineOptions dataflowOptions = pipelineOptions.as(DataflowPipelineOptions.class);
    String machineType = dataflowOptions.getWorkerMachineType();  // Returns null
} catch (Exception e) {
    // Exception caught, machine type not available
    return null;  // ← Returns null for local execution
}
```

**Result:**
- ⚠️ Machine type: `null` (not available)
- ✅ Uses local CPU cores for vCPUs
- ✅ Falls back to record-count-based scenarios

---

### **Scenario 3: Machine Type Not Set**

**When:** GCP Dataflow but machine type not explicitly set

**Process:**
```java
PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
DataflowPipelineOptions dataflowOptions = pipelineOptions.as(DataflowPipelineOptions.class);
// getWorkerMachineType() not called or returns null

String machineType = dataflowOptions.getWorkerMachineType();  // Returns null
```

**Result:**
- ⚠️ Machine type: `null` (not set)
- ✅ Falls back to record-count-based scenarios
- ✅ Uses default machine profile

---

## 🔄 Complete Detection Flow

```
┌─────────────────────────────────┐
│  ShardPlanner.calculateOptimal  │
│  ShardCount(pipelineOptions)   │
└──────────────┬──────────────────┘
               │
               ▼
┌─────────────────────────────────┐
│  EnvironmentDetector            │
│  .detectEnvironment()           │
└──────────────┬──────────────────┘
               │
       ┌───────┴───────┐
       │               │
       ▼               ▼
┌──────────────┐  ┌──────────────┐
│ detectMachine│  │ detectVirtual│
│ Type()       │  │ CPUs()       │
└──────┬───────┘  └──────┬───────┘
       │                 │
       ▼                 ▼
┌─────────────────────────────────┐
│  DataflowPipelineOptions         │
│  .getWorkerMachineType()        │
└──────────────┬──────────────────┘
               │
       ┌───────┴───────┐
       │               │
   Returns         Returns
   String          null
   (e.g.,          (local/
   "n2-            not set)
   standard-4")
       │               │
       ▼               ▼
┌─────────────────────────────────┐
│  ExecutionEnvironment            │
│  (machineType, vCPUs, workers)   │
└─────────────────────────────────┘
```

---

## 💻 Code Example

### **How to Set Machine Type in PipelineOptions:**

```java
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;

// Create PipelineOptions
PipelineOptions pipelineOptions = PipelineOptionsFactory.create();

// Cast to DataflowPipelineOptions
DataflowPipelineOptions dataflowOptions = pipelineOptions.as(DataflowPipelineOptions.class);

// Set machine type
dataflowOptions.setWorkerMachineType("n2-highcpu-8");  // ← Set machine type
dataflowOptions.setProject("my-gcp-project");
dataflowOptions.setRegion("us-central1");
dataflowOptions.setMaxNumWorkers(16);

// Pass to ShardPlanner
int shardCount = ShardPlanner.calculateOptimalShardCount(
    pipelineOptions,  // ← Contains machine type
    databasePoolMaxSize,
    estimatedRowCount,
    averageRowSizeBytes,
    null,
    null,
    null
);
```

---

## 📝 Machine Type Usage

Once detected, machine type is used for:

### **1. Machine Type-Based Optimization (PRIMARY)**

```java
if (environment.machineType != null && !environment.machineType.isBlank() && !environment.isLocalExecution) {
    // Machine type provided → use machine-type-based calculation
    log.info("Machine type provided ({}): using machine-type-based optimization", environment.machineType);
    optimizedShardCount = MachineTypeBasedOptimizer.optimizeBasedOnMachineType(...);
}
```

### **2. Machine Profile Selection**

```java
MachineProfile profile = MachineProfileProvider.getProfile(environment.machineType);

// Profile based on machine type:
// - High-CPU: maxShardsPerVcpu = 2
// - High-Memory: maxShardsPerVcpu = 1
// - Standard: maxShardsPerVcpu = 1
```

### **3. Resource Validation**

```java
// Validate user-provided values against machine type
int maxShards = MachineTypeResourceValidator.calculateMaxShardsForMachineType(
    environment, profile);
// maxShards = workers × vCPUs × maxShardsPerVcpu
```

### **4. vCPU Extraction**

```java
// Extract vCPUs from machine type string
// "n2-standard-4" → 4 vCPUs
// "n2-highcpu-8" → 8 vCPUs
String[] parts = machineType.split("-");
int vCPUs = Integer.parseInt(parts[parts.length - 1]);
```

---

## 🔍 Logging

The system logs machine type detection:

### **When Machine Type Detected:**
```
INFO: GCP Dataflow detected: machine type n2-standard-4 -> 4 vCPUs
INFO: Detected machine type: n2-standard-4
INFO: Machine type provided (n2-standard-4): using machine-type-based optimization
```

### **When Machine Type Not Available:**
```
DEBUG: Failed to detect machine type from PipelineOptions: ...
INFO: Local execution detected: using 8 vCPUs from available processors
INFO: Machine type not provided: falling back to record-count-based scenario optimization
```

---

## ⚠️ Important Notes

### **1. Local Execution:**
- Machine type is `null` for local execution
- System uses available CPU cores instead
- Falls back to record-count scenarios

### **2. Exception Handling:**
- If `DataflowPipelineOptions` cast fails → returns `null`
- If `getWorkerMachineType()` returns `null` → treated as local execution
- Graceful fallback to record-count scenarios

### **3. Machine Type Format:**
- Expected format: `{family}-{series}-{size}` (e.g., "n2-standard-4")
- vCPUs extracted from last part (e.g., "4" from "n2-standard-4")
- Invalid format → falls back to local CPU cores

---

## 📋 Summary

### **Machine Type Detection:**

1. **Source:** `PipelineOptions` → `DataflowPipelineOptions.getWorkerMachineType()`
2. **Method:** `EnvironmentDetector.detectMachineType()`
3. **Returns:** Machine type string (e.g., "n2-standard-4") or `null`
4. **Usage:** 
   - Machine-type-based optimization (if provided)
   - Resource validation
   - vCPU extraction
   - Profile selection

### **Detection Flow:**

```
PipelineOptions
  → DataflowPipelineOptions.getWorkerMachineType()
  → EnvironmentDetector.detectMachineType()
  → ExecutionEnvironment.machineType
  → Used for optimization and validation
```

**The ShardPlanner automatically detects machine type from PipelineOptions and uses it for optimization!**
