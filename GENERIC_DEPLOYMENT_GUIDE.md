# Generic Deployment Guide - Deploy to Any Environment

## 🎯 Overview

The system is now **fully generic and environment-agnostic**, making it easy to deploy and run in **any environment** (Local, GCP, AWS, etc.) with **minimal or zero configuration**.

---

## ✨ Key Features

### **1. Automatic Environment Detection**
- Automatically detects execution environment (Local, GCP, AWS)
- No manual configuration required
- Smart defaults for all environments

### **2. Zero-Config Deployment**
- Works out-of-the-box with sensible defaults
- Missing machine type → automatically defaults to LOCAL (safe default)
- Automatic resource detection (vCPUs, workers)

### **3. Flexible Configuration**
- Supports multiple configuration sources
- Priority-based configuration resolution
- Environment variables and system properties support

### **4. Environment-Agnostic**
- Same code works across all environments
- Environment-specific optimizations applied automatically
- Easy to add new cloud providers

---

## 🚀 Quick Start - Zero Configuration

### **Local Execution (Default)**

**No configuration needed!** Just run:

```bash
java -jar streamnova.jar
```

**What happens:**
- ✅ Automatically detects LOCAL execution
- ✅ Uses local CPU cores for vCPUs
- ✅ Uses local CPU cores for workers
- ✅ Applies LOCAL-optimized calculations
- ✅ Works immediately without any config

**Logs:**
```
INFO: Starting generic environment detection (automatic configuration)...
INFO: Cloud provider detected: LOCAL
INFO: Machine type not provided: automatically defaulting to LOCAL execution (safe default for easy deployment)
INFO: vCPUs for LOCAL execution: 8 (from available processors)
INFO: Workers for LOCAL execution: 8 (based on 8 vCPUs)
INFO: Deployment Mode: LOCAL - Optimized for local development
```

---

## 📋 Configuration Priority Order

The system uses a **priority-based configuration system** for maximum flexibility:

### **Machine Type Detection:**

```
1. Config File (pipeline_config.yml)          ← HIGHEST PRIORITY
   ↓ (if not provided)
2. PipelineOptions (DataflowPipelineOptions)  ← CLOUD-SPECIFIC
   ↓ (if not provided)
3. System Properties / Environment Variables  ← RUNTIME OVERRIDE
   ↓ (if not provided)
4. Auto-detect from cloud provider            ← AUTOMATIC
   ↓ (if not provided)
5. null → Defaults to LOCAL                   ← SAFE DEFAULT
```

### **vCPU Detection:**

```
1. System Property: streamnova.vcpus
   Environment Variable: STREAMNOVA_VCPUS
   ↓ (if not provided)
2. Extract from machine type (e.g., "n2-standard-4" → 4)
   ↓ (if not provided)
3. Local CPU cores (for LOCAL execution)
   ↓ (if not provided)
4. PipelineOptions detection
   ↓ (if not provided)
5. Safe default: Local CPU cores
```

### **Worker Detection:**

```
1. System Property: streamnova.workers
   Environment Variable: STREAMNOVA_WORKERS
   ↓ (if not provided)
2. Local: Based on vCPUs
   ↓ (if not provided)
3. PipelineOptions (GCP Dataflow, AWS, etc.)
   ↓ (if not provided)
4. Cloud provider defaults (GCP: auto-calculate, AWS: auto-calculate)
   ↓ (if not provided)
5. Safe default: Based on vCPUs
```

---

## 🌍 Deployment Scenarios

### **Scenario 1: Local Development (Zero Config)**

**Configuration:** None required

**What happens:**
- Detects LOCAL execution
- Uses local CPU cores
- Applies LOCAL-optimized calculations
- Works immediately

**Example:**
```bash
# Just run it!
java -jar streamnova.jar
```

---

### **Scenario 2: GCP Dataflow (Minimal Config)**

**Configuration:** Machine type in config file

```yaml
# pipeline_config.yml
pipeline:
  config:
    source:
      machineType: n2-standard-4  # Only this is required!
```

**What happens:**
- Detects GCP Dataflow from PipelineOptions
- Uses machine type from config
- Extracts vCPUs from machine type
- Applies GCP-optimized calculations
- Validates machine type requirement

**Example:**
```java
DataflowPipelineOptions options = PipelineOptionsFactory.create();
options.setProject("my-project");
options.setRegion("us-central1");
// Machine type from config file is used automatically
```

---

### **Scenario 3: GCP Dataflow (PipelineOptions Config)**

**Configuration:** Machine type in PipelineOptions

```java
DataflowPipelineOptions options = PipelineOptionsFactory.create();
options.setProject("my-project");
options.setRegion("us-central1");
options.setWorkerMachineType("n2-highcpu-8");  // Machine type here
```

**What happens:**
- Detects GCP Dataflow
- Uses machine type from PipelineOptions
- Extracts vCPUs from machine type
- Applies GCP-optimized calculations

---

### **Scenario 4: Runtime Override (System Properties)**

**Configuration:** Override via system properties

```bash
java -Dstreamnova.machine.type=n2-standard-4 \
     -Dstreamnova.vcpus=8 \
     -Dstreamnova.workers=4 \
     -jar streamnova.jar
```

**What happens:**
- Uses system property values (highest priority)
- Overrides any config file or PipelineOptions
- Useful for testing and dynamic configuration

---

### **Scenario 5: Environment Variables**

**Configuration:** Override via environment variables

```bash
export STREAMNOVA_MACHINE_TYPE=n2-standard-4
export STREAMNOVA_VCPUS=8
export STREAMNOVA_WORKERS=4
java -jar streamnova.jar
```

**What happens:**
- Uses environment variable values
- Overrides config file and PipelineOptions
- Useful for containerized deployments

---

## 🔧 Configuration Sources

### **1. Config File (pipeline_config.yml)**

```yaml
pipeline:
  config:
    source:
      machineType: n2-standard-4  # Optional
      # Other config...
```

**Priority:** Highest  
**Use Case:** Production deployments, version-controlled config

---

### **2. PipelineOptions**

```java
DataflowPipelineOptions options = PipelineOptionsFactory.create();
options.setWorkerMachineType("n2-standard-4");
```

**Priority:** High (after config file)  
**Use Case:** Programmatic configuration, GCP Dataflow

---

### **3. System Properties**

```bash
-Dstreamnova.machine.type=n2-standard-4
-Dstreamnova.vcpus=8
-Dstreamnova.workers=4
```

**Priority:** Very High (overrides config and PipelineOptions)  
**Use Case:** Runtime overrides, testing, dynamic configuration

---

### **4. Environment Variables**

```bash
export STREAMNOVA_MACHINE_TYPE=n2-standard-4
export STREAMNOVA_VCPUS=8
export STREAMNOVA_WORKERS=4
```

**Priority:** Very High (overrides config and PipelineOptions)  
**Use Case:** Containerized deployments, CI/CD pipelines

---

### **5. Automatic Detection**

**Priority:** Lowest (fallback)  
**Use Case:** Zero-config deployment, local development

---

## 🎯 Environment-Specific Behavior

### **LOCAL Execution**

**When:** Machine type not provided OR no cloud provider detected

**Defaults:**
- vCPUs: Local CPU cores
- Workers: Local CPU cores
- Machine Type: null (LOCAL)
- Cloud Provider: LOCAL

**Optimization:**
- Conservative calculations
- Profile-based: `vCPUs × 1.5` for shards
- Single worker

**Example:**
```
8 CPU cores → 8 vCPUs → 8 workers → ~12 shards (8 × 1.5)
```

---

### **GCP Dataflow**

**When:** Machine type provided AND GCP detected (project + region)

**Requirements:**
- ✅ Machine type MUST be provided (validated)
- ✅ Project and region in PipelineOptions

**Defaults:**
- vCPUs: Extracted from machine type
- Workers: From PipelineOptions or auto-calculate
- Machine Type: From config or PipelineOptions
- Cloud Provider: GCP

**Optimization:**
- Distributed calculations
- Machine-type-aware
- Cost-optimized

**Example:**
```
n2-standard-4 (4 vCPUs) × 4 workers → 32 shards (4 × 4 × 2)
```

---

### **AWS (Future Support)**

**When:** AWS detected (future)

**Defaults:**
- vCPUs: Extracted from instance type
- Workers: From PipelineOptions or auto-calculate
- Machine Type: From config or PipelineOptions
- Cloud Provider: AWS

**Optimization:**
- AWS-specific calculations (to be implemented)
- Instance-type-aware
- Cost-optimized

---

## 📊 Automatic Detection Flow

```
Start
  ↓
Detect Cloud Provider
  ├─→ GCP? (project + region set)
  ├─→ AWS? (AWS options set) [future]
  └─→ LOCAL (default)
  ↓
Detect Machine Type (Priority Order)
  ├─→ Config file?
  ├─→ PipelineOptions?
  ├─→ System property?
  └─→ null → Default to LOCAL
  ↓
Detect vCPUs
  ├─→ System property?
  ├─→ Extract from machine type?
  ├─→ Local CPU cores?
  └─→ Safe default
  ↓
Detect Workers
  ├─→ System property?
  ├─→ Local: Based on vCPUs?
  ├─→ PipelineOptions?
  └─→ Safe default
  ↓
Apply Environment-Specific Optimizations
  ├─→ LOCAL: Conservative
  ├─→ GCP: Distributed, cost-optimized
  └─→ AWS: AWS-optimized [future]
  ↓
Ready for Deployment!
```

---

## 🔍 Logging and Transparency

The system provides comprehensive logging for transparency:

```
INFO: Starting generic environment detection (automatic configuration)...
INFO: Cloud provider detected: LOCAL
INFO: Machine type not provided: automatically defaulting to LOCAL execution (safe default for easy deployment)
INFO: vCPUs for LOCAL execution: 8 (from available processors)
INFO: Workers for LOCAL execution: 8 (based on 8 vCPUs)
INFO: ═══════════════════════════════════════════════════════════════
INFO: Environment Detection Summary
INFO: ═══════════════════════════════════════════════════════════════
INFO: Cloud Provider: LOCAL
INFO: Machine Type: N/A (LOCAL execution)
INFO: Virtual CPUs: 8
INFO: Workers: 8
INFO: Local Execution: true
INFO: ═══════════════════════════════════════════════════════════════
INFO: Deployment Mode: LOCAL - Optimized for local development
INFO: Configuration: Minimal configuration required (auto-detected)
INFO: Environment detection complete: LOCAL environment ready for deployment
```

---

## ✅ Benefits

### **1. Zero-Config Deployment**
- Works immediately without configuration
- Safe defaults for all environments
- No manual setup required

### **2. Flexible Configuration**
- Multiple configuration sources
- Priority-based resolution
- Runtime overrides supported

### **3. Environment-Agnostic**
- Same code works everywhere
- Automatic environment detection
- Environment-specific optimizations

### **4. Easy Deployment**
- Deploy to any environment easily
- Minimal configuration required
- Production-ready defaults

### **5. Developer-Friendly**
- Works out-of-the-box locally
- No complex setup
- Clear logging and transparency

---

## 📝 Examples

### **Example 1: Local Development (Zero Config)**

```bash
# Just run it - no config needed!
java -jar streamnova.jar
```

**Result:** Automatically uses LOCAL execution with local CPU cores

---

### **Example 2: GCP Deployment (Minimal Config)**

```yaml
# pipeline_config.yml
pipeline:
  config:
    source:
      machineType: n2-standard-4  # Only this!
```

```java
DataflowPipelineOptions options = PipelineOptionsFactory.create();
options.setProject("my-project");
options.setRegion("us-central1");
// Machine type from config is used automatically
```

**Result:** Automatically detects GCP and uses machine type from config

---

### **Example 3: Runtime Override**

```bash
java -Dstreamnova.machine.type=n2-highcpu-8 \
     -Dstreamnova.vcpus=8 \
     -Dstreamnova.workers=4 \
     -jar streamnova.jar
```

**Result:** Uses system property values, overriding any config

---

### **Example 4: Containerized Deployment**

```dockerfile
ENV STREAMNOVA_MACHINE_TYPE=n2-standard-4
ENV STREAMNOVA_VCPUS=4
ENV STREAMNOVA_WORKERS=2
```

**Result:** Uses environment variables for configuration

---

## 🎯 Summary

| Feature | Description |
|---------|-------------|
| **Zero-Config** | Works out-of-the-box with sensible defaults |
| **Auto-Detection** | Automatically detects environment and resources |
| **Flexible Config** | Supports config file, PipelineOptions, system properties, environment variables |
| **Environment-Agnostic** | Same code works across Local, GCP, AWS, etc. |
| **Safe Defaults** | Missing config → defaults to LOCAL (safe, easy deployment) |
| **Production-Ready** | Optimized for each environment automatically |

---

## 🚀 Quick Deployment Checklist

### **Local Development:**
- ✅ No configuration needed
- ✅ Just run the application
- ✅ Automatically uses LOCAL execution

### **GCP Dataflow:**
- ✅ Add `machineType` to config file OR PipelineOptions
- ✅ Set `project` and `region` in PipelineOptions
- ✅ System automatically detects and optimizes

### **AWS (Future):**
- ✅ Add `machineType` to config file OR PipelineOptions
- ✅ Set AWS-specific options in PipelineOptions
- ✅ System automatically detects and optimizes

### **Any Environment:**
- ✅ Use system properties or environment variables for runtime overrides
- ✅ System automatically adapts to the environment
- ✅ Works seamlessly across all environments

---

**The system is now fully generic and can be easily deployed to any environment with minimal or zero configuration!** 🎉
