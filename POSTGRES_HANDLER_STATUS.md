# PostgresHandler Status

## ✅ Fixed: Registration Issue

**Problem:** `PostgresHandler` was missing, causing error:
```
Unsupported source type: 'postgres'. Available types: [mysql, hive, gcs, oracle]
```

**Solution:** ✅ Created `PostgresHandler.java` in `com.di.streamnova.handler.jdbc` package

**Status:** ✅ **REGISTERED** - Handler is now detected by `SourceHandlerRegistry`

---

## ⚠️ Current Implementation: Stub

**Location:** `src/main/java/com/di/streamnova/handler/jdbc/PostgresHandler.java`

**Current State:**
- ✅ Has `@Component` annotation (registered with Spring)
- ✅ Returns `"postgres"` from `type()` method
- ✅ Implements `SourceHandler<PipelineConfigSource>` interface
- ⚠️ `read()` method is a stub (throws `UnsupportedOperationException`)

---

## 📋 Required Implementation

The `PostgresHandler.read()` method needs to be fully implemented with:

### **1. Shard Calculation**
```java
import com.di.streamnova.util.shardplanner.ShardPlanner;
import com.di.streamnova.util.shardplanner.ShardWorkerPlan;

ShardWorkerPlan plan = ShardPlanner.calculateOptimalShardWorkerPlan(
    pipeline.getOptions(),
    config.getMaximumPoolSize(),
    estimatedRowCount,
    averageRowSizeBytes,
    null, // targetMbPerShard
    config.getShards(),
    config.getWorkers(),
    config.getMachineType()
);
int shardCount = plan.shardCount();
```

### **2. Statistics Estimation**
- Estimate table row count
- Calculate average row size
- Get table statistics for optimization

### **3. Schema Detection**
- Detect table schema from database
- Map PostgreSQL types to Beam Schema types
- Handle schema evolution

### **4. SQL Query Building**
- Build sharded SQL queries with hash-based partitioning
- PostgreSQL-specific syntax:
  - `md5()` function
  - `::text` and `::bit` type casting
  - Hash-based sharding expression

### **5. JDBC Read Execution**
- Create JDBC connection pool
- Execute parallel reads per shard
- Convert ResultSet to Beam Rows
- Handle errors and retries

### **6. Metrics Collection**
- Record read duration
- Track row counts
- Monitor connection failures

---

## 🔍 Missing Utility Classes

Based on documentation, PostgresHandler likely used these utility classes (may need to be created):

1. **TableStatisticsEstimator** - Estimate table statistics
2. **SchemaDetector** - Detect table schema
3. **SqlQueryBuilder** - Build sharded SQL queries
4. **DatabaseMetadataDiscoverer** - Discover shard expressions, order-by columns
5. **FetchSizeCalculator** - Calculate optimal fetch size
6. **ShardManager** - Create and validate shards
7. **SchemaBuilder** - Build Beam schemas

---

## ✅ Next Steps

1. **Immediate:** Handler is registered - error fixed ✅
2. **Short-term:** Implement full `read()` method
3. **Long-term:** Create utility classes if missing

---

## 📊 Current Status

- ✅ **Registration:** Fixed - Handler is registered
- ⚠️ **Implementation:** Stub - Needs full implementation
- ✅ **Compilation:** Success - Code compiles
- ⚠️ **Runtime:** Will throw `UnsupportedOperationException` when called

**The registration error is fixed, but the handler needs full implementation before it can be used in production.**
