# Performance Tracking Implementation Details

## Overview

Performance tracking is implemented using **Micrometer Timers** across all critical operations in both `ShardPlanner` and `PostgresHandler`.

## Timer Implementation Locations

### 1. **Shard planning metrics**

**Location:** `src/main/java/com/di/streamnova/util/MetricsCollector.java` records shard planning metrics. The agent uses `com.di.streamnova.agent.shardplanner.ShardPlanner.suggestShardCountForCandidate()` for candidate generation; when the pipeline runs, PostgresHandler uses the config (candidate-overridden or from YAML).

**Metric names:** `shardplanner.planning.duration`, `shardplanner.shard.count`, `shardplanner.planning.total` (see METRICS_GUIDE.md and METRICS_ENDPOINT_GUIDE.md).

**Implementation:** MetricsCollector is used where shard planning and Postgres read operations are executed; it records duration, shard count, and success/error.

**What it tracks:** Shard planning duration, shard count distribution, and success/error counts (see METRICS_GUIDE.md).

---

### 2. **PostgresHandler - Read Operation Duration**

**Location:** `src/main/java/com/di/streamnova/handler/jdbc/postgres/PostgresHandler.java`

**Timer Name:** `postgres.handler.read.duration`

**Implementation:**
```java
// Lines 64-150 in PostgresHandler.java
@Override
public PCollection<Row> read(Pipeline pipeline, PipelineConfigSource config) {
    long readStartTime = System.currentTimeMillis();  // ⏱️ Start timer
    
    try {
        // ... read operation logic ...
        
        PCollection<Row> result = executeJdbcRead(...);
        
        // Record successful read
        metricsCollector.recordPostgresRead(System.currentTimeMillis() - readStartTime);  // ⏱️ Record duration
        return result;
    } catch (Exception e) {
        metricsCollector.recordPostgresReadError();  // Record error
        throw e;
    }
}
```

**What it tracks:**
- Total time for complete PostgreSQL read operation
- Includes: statistics estimation, schema detection, shard calculation, and JDBC read
- Records P50, P95, P99 percentiles

---

### 3. **PostgresHandler - Schema Detection Duration**

**Location:** `src/main/java/com/di/streamnova/handler/jdbc/postgres/PostgresHandler.java`

**Timer Name:** `postgres.handler.schema.detection.duration`

**Implementation:**
```java
// Lines 120-125 in PostgresHandler.java
// Detect and build schema
long schemaStartTime = System.currentTimeMillis();  // ⏱️ Start timer
final Schema baseSchema = SchemaDetector.detectTableSchema(
        config.getDriver(), config.getJdbcUrl(), config.getUsername(), 
        config.getPassword(), tableName);
metricsCollector.recordSchemaDetection(System.currentTimeMillis() - schemaStartTime);  // ⏱️ Record duration
```

**What it tracks:**
- Time to detect table schema from database
- Includes database connection and metadata queries
- Helps identify slow schema detection operations

---

### 4. **PostgresHandler - Statistics Estimation Duration**

**Location:** `src/main/java/com/di/streamnova/handler/jdbc/postgres/PostgresHandler.java`

**Timer Name:** `postgres.handler.statistics.estimation.duration`

**Implementation:**
```java
// Lines 94-97 in PostgresHandler.java
// Estimate table statistics for optimization
long statsStartTime = System.currentTimeMillis();  // ⏱️ Start timer
final TableStatistics tableStatistics = TableStatisticsEstimator.estimateStatistics(
        config.getJdbcUrl(), config.getUsername(), config.getPassword(),
        schemaName, tableNameOnly, metricsCollector);
metricsCollector.recordStatisticsEstimation(System.currentTimeMillis() - statsStartTime);  // ⏱️ Record duration
```

**What it tracks:**
- Time to estimate row count and average row size
- Includes queries to `pg_class` and `pg_stats` tables
- Helps identify slow statistics queries

---

## Timer Registration

All timers are registered in `MetricsCollector.java`:

**Location:** `src/main/java/com/di/streamnova/util/MetricsCollector.java`

```java
// Lines 59-84
// Shard Planning Timer
this.shardPlanningTimer = Timer.builder("shardplanner.planning.duration")
        .description("Time taken to calculate optimal shard count")
        .register(meterRegistry);

// Postgres Read Timer
this.postgresReadTimer = Timer.builder("postgres.handler.read.duration")
        .description("Time taken to read data from PostgreSQL")
        .register(meterRegistry);

// Schema Detection Timer
this.schemaDetectionTimer = Timer.builder("postgres.handler.schema.detection.duration")
        .description("Time taken to detect table schema")
        .register(meterRegistry);

// Statistics Estimation Timer
this.statisticsEstimationTimer = Timer.builder("postgres.handler.statistics.estimation.duration")
        .description("Time taken to estimate table statistics")
        .register(meterRegistry);
```

## Timer Recording Methods

**Location:** `src/main/java/com/di/streamnova/util/MetricsCollector.java`

### 1. Shard Planning Timer
```java
// Line 118
shardPlanningTimer.record(durationMs, TimeUnit.MILLISECONDS);
```

### 2. Postgres Read Timer
```java
// Line 163
postgresReadTimer.record(durationMs, TimeUnit.MILLISECONDS);
```

### 3. Schema Detection Timer
```java
// Line 181
schemaDetectionTimer.record(durationMs, TimeUnit.MILLISECONDS);
```

### 4. Statistics Estimation Timer
```java
// Line 191
statisticsEstimationTimer.record(durationMs, TimeUnit.MILLISECONDS);
```

## What Metrics Are Collected

Each timer automatically collects:

1. **Count** - Total number of operations
2. **Total Time** - Sum of all durations
3. **Mean** - Average duration
4. **Max** - Maximum duration
5. **Percentiles** - P50, P95, P99 (if configured)

## Accessing Timer Metrics

### Via Actuator Endpoint

```bash
# Get shard planning duration metrics
curl http://localhost:8080/actuator/metrics/shardplanner.planning.duration

# Get postgres read duration metrics
curl http://localhost:8080/actuator/metrics/postgres.handler.read.duration

# Get schema detection duration metrics
curl http://localhost:8080/actuator/metrics/postgres.handler.schema.detection.duration

# Get statistics estimation duration metrics
curl http://localhost:8080/actuator/metrics/postgres.handler.statistics.estimation.duration
```

### Example Response

```json
{
  "name": "shardplanner.planning.duration",
  "description": "Time taken to calculate optimal shard count",
  "baseUnit": "milliseconds",
  "measurements": [
    {
      "statistic": "COUNT",
      "value": 10.0
    },
    {
      "statistic": "TOTAL_TIME",
      "value": 1250.0
    },
    {
      "statistic": "MAX",
      "value": 250.0
    }
  ]
}
```

### Via Prometheus Format

```bash
curl http://localhost:8080/actuator/prometheus | grep duration
```

Example output:
```
# HELP shardplanner_planning_duration_seconds Time taken to calculate optimal shard count
# TYPE shardplanner_planning_duration_seconds summary
shardplanner_planning_duration_seconds_count 10.0
shardplanner_planning_duration_seconds_sum 1.25
shardplanner_planning_duration_seconds_max 0.25
```

## Performance Tracking Flow

```
┌─────────────────────────────────────────────────────────────┐
│                    PostgresHandler.read()                     │
│                                                               │
│  ⏱️ Start: readStartTime                                      │
│  │                                                             │
│  ├─► ⏱️ Start: statsStartTime                                  │
│  │   └─► TableStatisticsEstimator.estimateStatistics()        │
│  │   ⏱️ Record: statisticsEstimationTimer                     │
│  │                                                             │
│  ├─► ShardPlanner.calculateOptimalShardCount()                │
│  │   ⏱️ Start: startTime (in ShardPlanner)                     │
│  │   └─► ⏱️ Record: shardPlanningTimer                        │
│  │                                                             │
│  ├─► ⏱️ Start: schemaStartTime                                 │
│  │   └─► SchemaDetector.detectTableSchema()                   │
│  │   ⏱️ Record: schemaDetectionTimer                           │
│  │                                                             │
│  └─► executeJdbcRead()                                        │
│      ⏱️ Record: postgresReadTimer (total operation)           │
└─────────────────────────────────────────────────────────────┘
```

## Summary

**4 Timers Implemented:**
1. ✅ `shardplanner.planning.duration` - Shard calculation time
2. ✅ `postgres.handler.read.duration` - Total read operation time
3. ✅ `postgres.handler.schema.detection.duration` - Schema detection time
4. ✅ `postgres.handler.statistics.estimation.duration` - Statistics estimation time

**All timers:**
- Track execution time in milliseconds
- Automatically calculate percentiles (P50, P95, P99)
- Record count, total time, mean, and max
- Available via Actuator endpoints and Prometheus format
