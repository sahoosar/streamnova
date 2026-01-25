# Profile-Based Calculation for Different Environments

## 🎯 Overview

`ProfileBasedCalculator` now has **environment-specific execution strategies** for:
- **Local Execution**: Optimized for local development
- **GCP Dataflow**: Optimized for Google Cloud Platform
- **AWS**: Prepared for future AWS support

Each environment uses different calculation strategies optimized for its characteristics.

---

## 🔄 Architecture

### **Cloud Provider Detection**

The system automatically detects the cloud provider:

```java
ExecutionEnvironment.CloudProvider cloudProvider = EnvironmentDetector.detectCloudProvider(pipelineOptions);
```

**Detection Logic:**
1. **GCP**: Checks for `DataflowPipelineOptions` with project and region set
2. **AWS**: Checks for AWS-specific options (future support)
3. **Local**: Default fallback when no cloud provider detected

### **Routing Logic**

```java
switch (environment.cloudProvider) {
    case LOCAL:
        return calculateForLocal(...);
    case GCP:
        return calculateForGcp(...);
    case AWS:
        return calculateForAws(...);
    default:
        return calculateForLocal(...);  // Fallback
}
```

---

## 🏠 Local Execution Strategy

### **Characteristics:**
- Single machine with limited resources
- Single worker (local execution)
- Conservative approach to avoid overwhelming the machine

### **Calculation Formula:**
```
baseShards = ceil(vCPUs × 1.5)
shardCount = min(baseShards, safePoolCapacity)
```

**Key Points:**
- Uses **1.5x multiplier** for conservative parallelism
- Based on available CPU cores (vCPUs)
- Respects database pool limits (80% headroom)
- Ensures at least 1 shard

### **Example:**
- **vCPUs**: 8
- **Database Pool**: 20
- **Calculation**: `ceil(8 × 1.5) = 12 shards`
- **Capped by pool**: `min(12, 16) = 12 shards` (80% of 20 = 16)
- **Final**: **12 shards**

### **Logging:**
```
INFO: Local execution profile calculation: vCPUs=8, workers=1
INFO: Local profile calculation: baseShards=12 (vCPUs=8 × 1.5), poolCap=16, finalShards=12
```

---

## ☁️ GCP Dataflow Strategy

### **Characteristics:**
- Multiple workers in distributed environment
- Machine type awareness (vCPUs per worker)
- Optimized for GCP Dataflow architecture

### **Calculation Formula:**
```
queriesPerWorker = ceil(vCPUs × jdbcConnectionsPerVcpu)
plannedConcurrency = workers × queriesPerWorker
safeConcurrency = min(plannedConcurrency, poolCapacity × 0.8)
shardCount = safeConcurrency × shardsPerQuery
```

**Key Points:**
- Uses **machine profile** (jdbcConnectionsPerVcpu, shardsPerQuery)
- Calculates **total concurrency** across all workers
- Respects database pool limits (80% headroom)
- Optimized for distributed processing

### **Example:**
- **Machine Type**: `n2-standard-4` (4 vCPUs)
- **Workers**: 4
- **Profile**: `jdbcConnectionsPerVcpu=1.0`, `shardsPerQuery=2.0`
- **Database Pool**: 50

**Calculation:**
1. `queriesPerWorker = ceil(4 × 1.0) = 4`
2. `plannedConcurrency = 4 × 4 = 16`
3. `safeConcurrency = min(16, 40) = 16` (80% of 50 = 40)
4. `shardCount = 16 × 2.0 = 32 shards`

**Final**: **32 shards**

### **Logging:**
```
INFO: GCP Dataflow profile calculation: machineType=n2-standard-4, vCPUs=4, workers=4
INFO: GCP profile calculation: queriesPerWorker=4, plannedConcurrency=16, safeConcurrency=16, 
      shardsPerQuery=2.0, rawShardCount=32, finalShards=32
```

---

## 🌐 AWS Strategy (Future Support)

### **Current Status:**
- **Placeholder implementation** using GCP-like calculation
- Ready for AWS-specific optimizations when AWS support is added

### **Planned Enhancements:**
- AWS instance type detection (e.g., `m5.xlarge`, `c5.2xlarge`)
- AWS-specific profile factors
- AWS-specific concurrency limits
- AWS-specific cost optimizations

### **Current Implementation:**
Uses GCP-like calculation as baseline:
```
queriesPerWorker = ceil(vCPUs × jdbcConnectionsPerVcpu)
plannedConcurrency = workers × queriesPerWorker
safeConcurrency = min(plannedConcurrency, poolCapacity × 0.8)
shardCount = safeConcurrency × shardsPerQuery
```

### **Logging:**
```
INFO: AWS execution profile calculation: machineType=..., vCPUs=..., workers=... 
      (Note: AWS support is in development, using GCP-like calculation)
INFO: AWS profile calculation (temporary): queriesPerWorker=..., plannedConcurrency=..., 
      safeConcurrency=..., rawShardCount=..., finalShards=...
```

---

## 📊 Comparison Table

| Aspect | Local | GCP | AWS (Future) |
|--------|-------|-----|--------------|
| **Base Calculation** | `vCPUs × 1.5` | `workers × queriesPerWorker` | `workers × queriesPerWorker` |
| **Multiplier** | 1.5 (conservative) | Profile-based | Profile-based (AWS-specific) |
| **Workers** | 1 (single machine) | Multiple (distributed) | Multiple (distributed) |
| **Concurrency** | Single-threaded | Multi-worker | Multi-worker |
| **Optimization** | Conservative | Distributed | Distributed (AWS-optimized) |
| **Pool Limit** | 80% headroom | 80% headroom | 80% headroom |

---

## 🔍 Code Structure

### **Main Entry Point:**
```java
ProfileBasedCalculator.calculateUsingProfile(environment, profile, databasePoolMaxSize)
```

### **Environment-Specific Methods:**
- `calculateForLocal()` - Local execution strategy
- `calculateForGcp()` - GCP Dataflow strategy
- `calculateForAws()` - AWS strategy (future)

### **Cloud Provider Detection:**
```java
EnvironmentDetector.detectCloudProvider(pipelineOptions)
```

---

## 📝 Usage Examples

### **Example 1: Local Execution**

```java
ExecutionEnvironment environment = EnvironmentDetector.detectEnvironment(pipelineOptions, null);
// environment.cloudProvider = CloudProvider.LOCAL
// environment.virtualCpus = 8 (from local CPU cores)
// environment.workerCount = 1

int shards = ProfileBasedCalculator.calculateUsingProfile(environment, profile, 20);
// Result: 12 shards (8 × 1.5 = 12, capped at 16)
```

### **Example 2: GCP Dataflow**

```java
DataflowPipelineOptions dataflowOptions = pipelineOptions.as(DataflowPipelineOptions.class);
dataflowOptions.setProject("my-project");
dataflowOptions.setRegion("us-central1");
dataflowOptions.setWorkerMachineType("n2-standard-4");

ExecutionEnvironment environment = EnvironmentDetector.detectEnvironment(pipelineOptions, null);
// environment.cloudProvider = CloudProvider.GCP
// environment.virtualCpus = 4
// environment.workerCount = 4

int shards = ProfileBasedCalculator.calculateUsingProfile(environment, profile, 50);
// Result: 32 shards (4 workers × 4 queries × 2 shardsPerQuery = 32)
```

### **Example 3: AWS (Future)**

```java
// When AWS support is added:
AwsPipelineOptions awsOptions = pipelineOptions.as(AwsPipelineOptions.class);
awsOptions.setAwsRegion("us-east-1");
awsOptions.setInstanceType("m5.xlarge");

ExecutionEnvironment environment = EnvironmentDetector.detectEnvironment(pipelineOptions, null);
// environment.cloudProvider = CloudProvider.AWS
// environment.virtualCpus = 4
// environment.workerCount = 4

int shards = ProfileBasedCalculator.calculateUsingProfile(environment, profile, 50);
// Result: AWS-optimized calculation (to be implemented)
```

---

## 🔧 Extending for AWS

When AWS support is added, update `calculateForAws()`:

```java
private static int calculateForAws(ExecutionEnvironment environment, 
                                  MachineProfile profile, 
                                  Integer databasePoolMaxSize) {
    // 1. Detect AWS instance type (e.g., m5.xlarge, c5.2xlarge)
    String instanceType = detectAwsInstanceType(environment);
    
    // 2. Get AWS-specific profile factors
    AwsProfile awsProfile = AwsProfileProvider.getProfile(instanceType);
    
    // 3. Calculate with AWS-specific optimizations
    int queriesPerWorker = calculateAwsQueriesPerWorker(environment, awsProfile);
    
    // 4. Apply AWS-specific concurrency limits
    int maxConcurrency = awsProfile.getMaxConcurrency();
    
    // 5. Calculate shards with AWS optimizations
    // ... AWS-specific calculation logic ...
    
    return shardCount;
}
```

---

## ✅ Benefits

1. **Environment-Optimized**: Each environment uses the best strategy for its characteristics
2. **Extensible**: Easy to add new cloud providers (AWS, Azure, etc.)
3. **Maintainable**: Clear separation of concerns per environment
4. **Production-Ready**: GCP strategy optimized for distributed processing
5. **Development-Friendly**: Local strategy conservative for local machines

---

## 📋 Summary

| Environment | Strategy | Key Formula | Use Case |
|------------|----------|-------------|----------|
| **Local** | Conservative | `vCPUs × 1.5` | Local development |
| **GCP** | Distributed | `workers × queriesPerWorker × shardsPerQuery` | Production GCP deployments |
| **AWS** | Distributed (future) | Similar to GCP (to be enhanced) | Production AWS deployments |

**Profile-based calculation is now environment-aware and optimized for each cloud provider!** 🚀
