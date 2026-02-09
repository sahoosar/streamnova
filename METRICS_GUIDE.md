# Metrics Guide for ShardPlanner and PostgresHandler

## Overview

Metrics are recorded via **MetricsCollector** (`com.di.streamnova.util.MetricsCollector`) using **Micrometer** (Spring Boot Actuator). Shard planning in the agent uses `agent.shardplanner.ShardPlanner.suggestShardCountForCandidate()`; metrics are recorded where planning and Postgres read operations run.

## Available Metrics

### Shard Planning Metrics

| Metric Name | Type | Description | Tags |
|------------|------|-------------|------|
| `shardplanner.shard.count` | DistributionSummary | Distribution of calculated shard counts | - |
| `shardplanner.planning.duration` | Timer | Time taken to calculate optimal shard count | - |
| `shardplanner.planning.total` | Counter | Total shard planning operations | `status=success` or `status=error` |

**Example Queries:**
```promql
# Average shard count
rate(shardplanner_shard_count_sum[5m]) / rate(shardplanner_shard_count_count[5m])

# P95 planning duration
histogram_quantile(0.95, rate(shardplanner_planning_duration_bucket[5m]))

# Error rate
rate(shardplanner_planning_total{status="error"}[5m])
```

### Postgres Handler Metrics

| Metric Name | Type | Description | Tags |
|------------|------|-------------|------|
| `postgres.handler.read.duration` | Timer | Time taken to read data from PostgreSQL | - |
| `postgres.handler.read.total` | Counter | Total read operations | `status=success` or `status=error` |
| `postgres.handler.schema.detection.duration` | Timer | Time taken to detect table schema | - |
| `postgres.handler.statistics.estimation.duration` | Timer | Time taken to estimate table statistics | - |
| `postgres.handler.connection.failures` | Counter | Number of database connection failures | - |

**Example Queries:**
```promql
# Average read duration
rate(postgres_handler_read_duration_sum[5m]) / rate(postgres_handler_read_duration_count[5m])

# Error rate
rate(postgres_handler_read_total{status="error"}[5m]) / rate(postgres_handler_read_total[5m])

# Connection failure rate
rate(postgres_handler_connection_failures[5m])
```

### Execution Environment Metrics

| Metric Name | Type | Description | Unit |
|------------|------|-------------|------|
| `execution.environment.vcpus` | DistributionSummary | Distribution of virtual CPUs detected | - |
| `execution.environment.workers` | DistributionSummary | Distribution of worker counts | - |
| `postgres.handler.estimated.row.count` | DistributionSummary | Distribution of estimated row counts | rows |

**Example Queries:**
```promql
# Average vCPUs
rate(execution_environment_vcpus_sum[5m]) / rate(execution_environment_vcpus_count[5m])

# Average estimated row count
rate(postgres_handler_estimated_row_count_sum[5m]) / rate(postgres_handler_estimated_row_count_count[5m])
```

## Accessing Metrics

### Via Spring Boot Actuator

1. **Enable Actuator endpoints** in `application.properties`:
```properties
management.endpoints.web.exposure.include=metrics,prometheus
management.metrics.export.prometheus.enabled=true
```

2. **Access metrics endpoint:**
```bash
# All metrics
curl http://localhost:8080/actuator/metrics

# Specific metric
curl http://localhost:8080/actuator/metrics/shardplanner.shard.count

# Prometheus format
curl http://localhost:8080/actuator/prometheus
```

### Via Prometheus

If you have Prometheus configured:

1. **Add Prometheus dependency** (if not already present):
```xml
<dependency>
    <groupId>io.micrometer</groupId>
    <artifactId>micrometer-registry-prometheus</artifactId>
</dependency>
```

2. **Scrape endpoint:** `http://your-app:8080/actuator/prometheus`

## Monitoring Dashboards

### Recommended Grafana Dashboard Panels

1. **Shard Planning Performance**
   - Shard count distribution (histogram)
   - Planning duration (P50, P95, P99)
   - Error rate

2. **Postgres Handler Performance**
   - Read operation duration (P50, P95, P99)
   - Schema detection time
   - Statistics estimation time
   - Connection failure rate

3. **Execution Environment**
   - vCPU distribution
   - Worker count distribution
   - Estimated row count distribution

## Alerting Recommendations

### Critical Alerts

```yaml
# High error rate in shard planning
- alert: HighShardPlanningErrorRate
  expr: rate(shardplanner_planning_total{status="error"}[5m]) > 0.1
  for: 5m
  annotations:
    summary: "High error rate in shard planning"

# High connection failure rate
- alert: HighConnectionFailureRate
  expr: rate(postgres_handler_connection_failures[5m]) > 1
  for: 5m
  annotations:
    summary: "High database connection failure rate"

# Slow read operations
- alert: SlowPostgresReads
  expr: histogram_quantile(0.95, rate(postgres_handler_read_duration_bucket[5m])) > 30000
  for: 10m
  annotations:
    summary: "P95 read duration exceeds 30 seconds"
```

### Warning Alerts

```yaml
# Increasing planning duration
- alert: IncreasingPlanningDuration
  expr: rate(shardplanner_planning_duration_sum[5m]) / rate(shardplanner_planning_duration_count[5m]) > 1000
  for: 15m
  annotations:
    summary: "Average planning duration exceeds 1 second"
```

## Implementation Details

### MetricsCollector Component

The `MetricsCollector` class is a Spring `@Component` that:
- Automatically registers all metrics with Micrometer
- Provides methods to record metrics from both classes
- Handles null-safety (metrics are optional)

### Integration Points

1. **ShardPlanner**: Records metrics via static setter injection
2. **PostgresHandler**: Records metrics via constructor injection

### Null Safety

Both classes handle cases where `MetricsCollector` might not be available:
- `ShardPlanner` checks `if (metricsCollector != null)` before recording
- `PostgresHandler` always has it injected (Spring component)

## Troubleshooting

### Metrics Not Appearing

1. **Check Spring Boot Actuator is enabled:**
   ```properties
   management.endpoints.web.exposure.include=*
   ```

2. **Verify MetricsCollector is being injected:**
   - Check application logs for Spring component initialization
   - Ensure `@Component` annotation is present

3. **Check Micrometer is on classpath:**
   ```bash
   mvn dependency:tree | grep micrometer
   ```

### High Cardinality Issues

If you see high cardinality in metrics:
- Review tag usage (currently minimal)
- Consider removing or limiting distribution summaries
- Use sampling for high-volume operations

## Future Enhancements

Potential additions:
- [ ] Per-table metrics (add table name as tag)
- [ ] Per-environment metrics (local vs GCP)
- [ ] Connection pool metrics (HikariCP integration)
- [ ] Shard distribution metrics (records per shard)
- [ ] Cost metrics (estimated cost per operation)
