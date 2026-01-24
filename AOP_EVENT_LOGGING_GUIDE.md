# AOP-Based Event Logging Guide

## Overview

`LoadOperationEventLogger` can be **replaced or enhanced** with AOP (Aspect-Oriented Programming) for automatic event logging. AOP provides:

- ✅ **Automatic logging** - No manual method calls needed
- ✅ **MDC support** - Automatically reads and preserves MDC context
- ✅ **Cleaner code** - Remove boilerplate event logging calls
- ✅ **Consistent logging** - All annotated methods logged the same way
- ✅ **Exception handling** - Automatic error event logging

## MDC Support in AOP

**Yes, AOP fully supports MDC!** 

- ✅ **MDC is thread-local** - AOP aspects run in the same thread as the intercepted method
- ✅ **Automatic access** - `MDC.get("jobId")` works directly in AOP aspects
- ✅ **No propagation needed** - MDC context is automatically available
- ✅ **Thread-safe** - Each thread has its own MDC context

## Implementation

### 1. **Add Spring AOP Dependency**

Already added to `pom.xml`:
```xml
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-aop</artifactId>
</dependency>
```

### 2. **Create Annotation**

`@LogLoadOperation` annotation marks methods for automatic logging:

```java
@LogLoadOperation(
    eventType = "POSTGRES_READ",
    operationContext = "postgres_read",
    parameterNames = {"tableName", "jdbcUrl"}
)
public PCollection<Row> read(Pipeline pipeline, PipelineConfigSource config) {
    // Method implementation - events logged automatically!
}
```

### 3. **AOP Aspect**

`LoadOperationEventAspect` automatically:
- Intercepts annotated methods
- Logs start event before execution
- Logs completion event after success
- Logs failure event on exception
- Extracts method parameters for context
- Preserves MDC (jobId, etc.)

## Usage Examples

### Example 1: PostgresHandler.read()

**Before (Manual):**
```java
public PCollection<Row> read(Pipeline pipeline, PipelineConfigSource config) {
    long readStartTime = System.currentTimeMillis();
    try {
        // ... implementation ...
        eventLogger.logLoadStarted("postgres", tableName, jdbcUrl);
        // ... more logging calls ...
        eventLogger.logLoadCompleted("postgres", tableName, duration, ...);
        return result;
    } catch (Exception e) {
        eventLogger.logLoadFailed("postgres", tableName, ...);
        throw e;
    }
}
```

**After (AOP):**
```java
@LogLoadOperation(
    eventType = "POSTGRES_READ",
    operationContext = "postgres_read",
    parameterNames = {"tableName", "jdbcUrl", "sourceType"}
)
public PCollection<Row> read(Pipeline pipeline, PipelineConfigSource config) {
    // ... implementation only - events logged automatically!
    return result;
}
```

### Example 2: Statistics Estimation

**Before:**
```java
long statsStartTime = System.currentTimeMillis();
final TableStatistics stats = estimateStatistics(...);
long duration = System.currentTimeMillis() - statsStartTime;
eventLogger.logStatisticsEstimated(tableName, stats.estimatedRowCount, ...);
```

**After:**
```java
@LogLoadOperation(
    eventType = "STATISTICS_ESTIMATED",
    operationContext = "statistics_estimation",
    parameterNames = {"tableName"}
)
private TableStatistics estimateStatistics(...) {
    // ... implementation - events logged automatically!
}
```

## MDC Handling

### Automatic MDC Access

AOP aspects automatically have access to MDC:

```java
@Around("@annotation(LogLoadOperation)")
public Object logLoadOperation(ProceedingJoinPoint joinPoint) {
    // MDC is automatically available - thread-local context
    String jobId = MDC.get("jobId"); // ✅ Works!
    String userId = MDC.get("userId"); // ✅ Works!
    
    // MDC context is preserved across method execution
    // No need to manually propagate
}
```

### MDC Propagation

**Important:** MDC is **thread-local** and **automatically available** in AOP aspects:

- ✅ **Same thread** - AOP aspects run in the same thread as the intercepted method
- ✅ **No propagation needed** - MDC context is automatically available
- ✅ **Thread-safe** - Each thread has its own MDC context

**For distributed systems (Apache Beam workers):**
- MDC does **not** automatically propagate to worker threads
- You may need to manually set MDC in worker threads
- Or pass jobId explicitly to worker functions

## Event Structure

AOP-generated events have the same structure as manual events:

```json
{
  "eventType": "POSTGRES_READ_STARTED",
  "timestamp": "2026-01-23T21:43:15.503Z",
  "applicationId": "StreamNova-4f462998",
  "jobId": "job-abc123",  // ✅ From MDC
  "threadId": 16,
  "threadName": "restartedMain",
  "context": {
    "operationContext": "postgres_read",
    "tableName": "market_summary",
    "jdbcUrl": "jdbc:postgresql://localhost:5432/marketdb",
    "method": "read",
    "className": "PostgresHandler"
  }
}
```

## Benefits of AOP Approach

### 1. **Reduced Boilerplate**

**Before:** ~10-15 lines of logging code per method
**After:** 1 annotation line

### 2. **Consistency**

All methods logged the same way automatically.

### 3. **Separation of Concerns**

Business logic separated from logging logic.

### 4. **Easy to Enable/Disable**

Remove annotation to disable logging.

### 5. **Automatic Exception Handling**

Error events logged automatically on exceptions.

## Migration Strategy

### Option 1: Hybrid Approach (Recommended)

Keep both approaches:
- Use AOP for new methods
- Gradually migrate existing methods
- Both work together seamlessly

### Option 2: Full Migration

Replace all manual logging with AOP:
1. Add `@LogLoadOperation` annotations
2. Remove manual `eventLogger` calls
3. Keep `LoadOperationEventLogger` for AOP to use

### Option 3: AOP Only

Remove `LoadOperationEventLogger` manual calls:
- Use only AOP-based logging
- Simpler codebase
- Less flexibility for custom events

## Configuration

### Enable AOP

AOP is automatically enabled when `spring-boot-starter-aop` is in classpath.

### Aspect Configuration

The aspect is automatically discovered as a `@Component`:

```java
@Aspect
@Component
public class LoadOperationEventAspect {
    // Automatically enabled
}
```

## Comparison: Manual vs AOP

| Feature | Manual (Current) | AOP (Proposed) |
|---------|------------------|----------------|
| **Code Lines** | ~10-15 per method | 1 annotation line |
| **MDC Support** | ✅ Manual access | ✅ Automatic access |
| **Exception Handling** | Manual try-catch | ✅ Automatic |
| **Duration Tracking** | Manual timing | ✅ Automatic |
| **Parameter Extraction** | Manual | ✅ Automatic |
| **Flexibility** | High (custom logic) | Medium (annotation-based) |
| **Consistency** | Manual (varies) | ✅ Automatic (consistent) |
| **Maintenance** | Higher (scattered) | Lower (centralized) |

## Example: Complete Migration

### PostgresHandler.read() with AOP

```java
@LogLoadOperation(
    eventType = "POSTGRES_READ",
    operationContext = "postgres_read",
    parameterNames = {"tableName", "jdbcUrl", "sourceType"}
)
@Override
public PCollection<Row> read(Pipeline pipeline, PipelineConfigSource config) {
    // Validate inputs
    final String tableName = InputValidator.validateTableName(config.getTable());
    InputValidator.validateJdbcUrl(config.getJdbcUrl());
    
    // Build database configuration
    final DbConfigSnapshot databaseConfigSnapshot = DatabaseConfigBuilder.buildConfigSnapshot(config);
    
    // Extract schema and table name
    final String[] schemaTableParts = TableNameUtils.splitSchemaAndTable(tableName);
    final String schemaName = InputValidator.validateSchemaName(schemaTableParts[0]);
    final String tableNameOnly = InputValidator.validateIdentifier(schemaTableParts[1], "Table name");
    
    // Estimate statistics (also can use AOP)
    final TableStatistics tableStatistics = TableStatisticsEstimator.estimateStatistics(...);
    
    // Calculate shard count
    final int shardCount = calculateShardCount(pipeline, config, tableStatistics);
    
    // ... rest of implementation ...
    
    // No manual logging needed - AOP handles it!
    return result;
}
```

## Best Practices

### 1. **Use Descriptive Event Types**

```java
@LogLoadOperation(eventType = "POSTGRES_READ")  // ✅ Good
@LogLoadOperation(eventType = "READ")            // ❌ Too generic
```

### 2. **Include Operation Context**

```java
@LogLoadOperation(
    eventType = "POSTGRES_READ",
    operationContext = "postgres_read"  // ✅ Helps filtering
)
```

### 3. **Specify Important Parameters**

```java
@LogLoadOperation(
    eventType = "POSTGRES_READ",
    parameterNames = {"tableName", "jdbcUrl"}  // ✅ Only important ones
)
```

### 4. **Don't Over-Annotate**

Only annotate methods that represent significant operations:
- ✅ Main handler methods
- ✅ Statistics estimation
- ✅ Schema detection
- ❌ Small utility methods
- ❌ Getters/setters

## Troubleshooting

### Issue: Events Not Logged

**Check:**
1. AOP is enabled (`spring-boot-starter-aop` in classpath)
2. Aspect is a `@Component`
3. Method is annotated with `@LogLoadOperation`
4. Method is called on a Spring-managed bean

### Issue: MDC Not Available

**Check:**
1. MDC is set before method call: `MDC.put("jobId", jobId)`
2. Method is called in the same thread (not in worker thread)
3. MDC is not cleared before method execution

### Issue: Parameters Not Extracted

**Check:**
1. Parameter names specified in `parameterNames`
2. Parameters are not null
3. Parameter types are serializable

## Summary

✅ **AOP can replace manual event logging**
✅ **MDC is fully supported** - automatically available in AOP aspects
✅ **Reduces boilerplate** - from 10-15 lines to 1 annotation
✅ **Automatic exception handling** - error events logged automatically
✅ **Thread-safe** - MDC context automatically preserved
✅ **Easy migration** - can use both approaches together

**Recommendation:** Use AOP for new code, gradually migrate existing code. Both approaches work together seamlessly!
