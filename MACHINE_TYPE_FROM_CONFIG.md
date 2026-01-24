# Machine Type from Config File

## 🎯 Overview

The `ShardPlanner` now supports reading machine type from `pipeline_config.yml` file. Users can specify machine type in the configuration, and it will take **priority** over machine type from `PipelineOptions`.

---

## 📋 Configuration

### **Add Machine Type to Config File:**

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
      
      # User Control Options:
      shards: 0      # optional - user-provided shard count (0 = calculate automatically)
      workers: 0     # optional - user-provided worker count (0 = calculate automatically)
      machineType: n2-highcpu-8  # optional - user-provided machine type
      fetchFactor: 1.0
```

---

## 🔄 Priority Order

### **Machine Type Detection Priority:**

```
1. Config File (pipeline_config.yml) ← HIGHEST PRIORITY
   ↓ (if not provided)
2. PipelineOptions (DataflowPipelineOptions.getWorkerMachineType())
   ↓ (if not provided)
3. null (local execution or not set)
```

**Key Principle:** Machine type from config file **always takes precedence** over PipelineOptions.

---

## 🔍 How It Works

### **Step 1: Read from Config File**

`PostgresHandler` reads machine type from `PipelineConfigSource`:

```java
// In PostgresHandler.calculateShardCount()
String userProvidedMachineType = null;
if (config.getMachineType() != null && !config.getMachineType().isBlank()) {
    userProvidedMachineType = config.getMachineType();
    log.info("Using user-provided machine type from config: {}", userProvidedMachineType);
}
```

### **Step 2: Pass to ShardPlanner**

Machine type from config is passed to `ShardPlanner`:

```java
int shardCount = ShardPlanner.calculateOptimalShardCount(
    pipeline.getOptions(), 
    config.getMaximumPoolSize(),
    statistics.estimatedRowCount, 
    statistics.averageRowSizeBytes, 
    null,
    userProvidedShardCount,
    userProvidedWorkerCount,
    userProvidedMachineType);  // ← Pass machine type from config
```

### **Step 3: Environment Detection**

`EnvironmentDetector` checks config machine type first:

```java
static ExecutionEnvironment detectEnvironment(
    PipelineOptions pipelineOptions, 
    String userProvidedMachineType) {
    
    // PRIORITY 1: User-provided machine type from config (highest priority)
    String machineType = userProvidedMachineType != null && !userProvidedMachineType.isBlank() 
            ? userProvidedMachineType 
            : detectMachineType(pipelineOptions);  // Fallback to PipelineOptions
    
    int virtualCpus = detectVirtualCpus(pipelineOptions, machineType);
    int workerCount = detectWorkerCount(pipelineOptions);
    return new ExecutionEnvironment(machineType, virtualCpus, workerCount);
}
```

---

## 📊 Examples

### **Example 1: Machine Type from Config (Takes Priority)**

**Config File:**
```yaml
pipeline:
  config:
    source:
      machineType: n2-highcpu-8  # ← User provides machine type
```

**PipelineOptions:**
```java
dataflowOptions.setWorkerMachineType("n2-standard-4");  // ← Also set in PipelineOptions
```

**Result:**
```
INFO: Using user-provided machine type from config: n2-highcpu-8
INFO: Machine type provided (n2-highcpu-8): using machine-type-based optimization
```

**Final:** Uses `n2-highcpu-8` from config (ignores PipelineOptions)

---

### **Example 2: Machine Type from PipelineOptions (Fallback)**

**Config File:**
```yaml
pipeline:
  config:
    source:
      machineType:   # ← Not provided (empty/null)
```

**PipelineOptions:**
```java
dataflowOptions.setWorkerMachineType("n2-standard-4");  // ← Set in PipelineOptions
```

**Result:**
```
INFO: Machine type provided (n2-standard-4): using machine-type-based optimization
```

**Final:** Uses `n2-standard-4` from PipelineOptions

---

### **Example 3: No Machine Type (Fallback to Scenarios)**

**Config File:**
```yaml
pipeline:
  config:
    source:
      machineType:   # ← Not provided
```

**PipelineOptions:**
```java
// No machine type set
```

**Result:**
```
INFO: Machine type not provided: falling back to record-count-based scenario optimization
```

**Final:** Falls back to record-count-based scenarios

---

## 🔧 Implementation Details

### **1. Config File Structure:**

**File:** `src/main/java/com/di/streamnova/config/PipelineConfigSource.java`

```java
@Data
@NoArgsConstructor
public class PipelineConfigSource {
    // ... other fields ...
    private String machineType;    // ← New field for machine type from config
}
```

### **2. YAML Configuration:**

**File:** `src/main/resources/pipeline_config.yml`

```yaml
pipeline:
  config:
    source:
      machineType: n2-highcpu-8  # ← User can specify machine type
```

### **3. Detection Logic:**

**File:** `src/main/java/com/di/streamnova/util/ShardPlanner.java`

```java
// Priority 1: Config file
String machineType = userProvidedMachineType != null && !userProvidedMachineType.isBlank() 
        ? userProvidedMachineType 
        : detectMachineType(pipelineOptions);  // Priority 2: PipelineOptions
```

---

## 📝 Supported Machine Types

Users can specify any GCP machine type in the config file:

### **Standard Machines:**
- `n2-standard-4` (4 vCPUs)
- `n2-standard-8` (8 vCPUs)
- `n2-standard-16` (16 vCPUs)

### **High-CPU Machines:**
- `n2-highcpu-8` (8 vCPUs)
- `n2-highcpu-16` (16 vCPUs)
- `n2-highcpu-32` (32 vCPUs)

### **High-Memory Machines:**
- `n2-highmem-4` (4 vCPUs)
- `n2-highmem-8` (8 vCPUs)
- `n2-highmem-16` (16 vCPUs)

---

## ✅ Benefits

### **1. User Control:**
- Users can specify machine type in config file
- No need to modify PipelineOptions code
- Easy to change for different environments

### **2. Priority System:**
- Config file takes highest priority
- PipelineOptions as fallback
- Graceful degradation to scenarios

### **3. Flexibility:**
- Works with or without machine type in config
- Works with or without machine type in PipelineOptions
- Supports all GCP machine types

---

## 🔍 Logging

The system logs machine type detection:

### **When Machine Type from Config:**
```
INFO: Using user-provided machine type from config: n2-highcpu-8
INFO: Machine type provided (n2-highcpu-8): using machine-type-based optimization
```

### **When Machine Type from PipelineOptions:**
```
INFO: Machine type provided (n2-standard-4): using machine-type-based optimization
```

### **When No Machine Type:**
```
INFO: Machine type not provided: falling back to record-count-based scenario optimization
```

---

## 📋 Summary

### **Machine Type Detection Priority:**

1. **Config File** (`pipeline_config.yml`) ← **HIGHEST PRIORITY**
2. **PipelineOptions** (`DataflowPipelineOptions.getWorkerMachineType()`) ← **FALLBACK**
3. **null** (local execution or not set) ← **FALLBACK TO SCENARIOS**

### **Usage:**

```yaml
pipeline:
  config:
    source:
      machineType: n2-highcpu-8  # ← Specify machine type here
```

**The ShardPlanner now reads machine type from config file with highest priority!**
