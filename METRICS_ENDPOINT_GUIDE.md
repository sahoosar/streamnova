# Metrics Endpoint Access Guide

## Quick Fix

The error you're seeing is because Actuator endpoints are not enabled. I've updated `application.properties` to fix this.

## Correct Endpoint URLs

After restarting your application, use these URLs:

### 1. List All Available Metrics
```bash
curl http://localhost:8080/actuator/metrics
```

### 2. Get Specific Metric Details
```bash
# Note: Use the exact metric name as registered
curl http://localhost:8080/actuator/metrics/shardplanner.shard.count
```

### 3. Get All Metrics in Prometheus Format
```bash
curl http://localhost:8080/actuator/prometheus
```

### 4. Health Check
```bash
curl http://localhost:8080/actuator/health
```

## Common Issues & Solutions

### Issue 1: 404 Not Found
**Problem:** Endpoints not enabled

**Solution:** Ensure `application.properties` contains:
```properties
management.endpoints.web.exposure.include=metrics,prometheus
```

### Issue 2: Metric Name Not Found
**Problem:** Metric hasn't been created yet (no operations have run)

**Solution:** 
1. Run a pipeline operation first to generate metrics
2. Then check available metrics: `curl http://localhost:8080/actuator/metrics`

### Issue 3: Wrong URL Format
**Problem:** Using incorrect URL path

**Correct Format:**
- ✅ `http://localhost:8080/actuator/metrics`
- ✅ `http://localhost:8080/actuator/metrics/shardplanner.shard.count`
- ❌ `http://localhost:8080/actuator/metrics/shardplanner.shard.count/` (trailing slash)
- ❌ `http://localhost:8080/actuator/metrics/shardplanner_shard_count` (wrong separator)

## Available Metrics (After Running Operations)

Once you've run at least one pipeline operation, these metrics will be available:

### Shard Planning Metrics
- `shardplanner.shard.count` - Distribution of shard counts
- `shardplanner.planning.duration` - Planning duration timer
- `shardplanner.planning.total` - Planning operation counter

### Postgres Handler Metrics
- `postgres.handler.read.duration` - Read operation timer
- `postgres.handler.read.total` - Read operation counter
- `postgres.handler.schema.detection.duration` - Schema detection timer
- `postgres.handler.statistics.estimation.duration` - Statistics estimation timer
- `postgres.handler.connection.failures` - Connection failure counter

### Environment Metrics
- `execution.environment.vcpus` - vCPU distribution
- `execution.environment.workers` - Worker count distribution
- `postgres.handler.estimated.row.count` - Estimated row count distribution

## Testing the Endpoints

### Step 1: Restart Application
After updating `application.properties`, restart your Spring Boot application.

### Step 2: Verify Actuator is Working
```bash
curl http://localhost:8080/actuator/health
```
Should return: `{"status":"UP"}`

### Step 3: List Available Metrics
```bash
curl http://localhost:8080/actuator/metrics
```
This will show all registered metrics (may be empty if no operations have run).

### Step 4: Run a Pipeline Operation
Execute your pipeline to generate metrics.

### Step 5: Check Metrics Again
```bash
curl http://localhost:8080/actuator/metrics
```
Now you should see your custom metrics listed.

### Step 6: Get Specific Metric
```bash
curl http://localhost:8080/actuator/metrics/shardplanner.shard.count
```

### Step 7: Get Prometheus Format
```bash
curl http://localhost:8080/actuator/prometheus
```
This returns all metrics in Prometheus format for scraping.

## Example Response

### List Metrics Response
```json
{
  "names": [
    "shardplanner.shard.count",
    "shardplanner.planning.duration",
    "postgres.handler.read.duration",
    ...
  ]
}
```

### Specific Metric Response
```json
{
  "name": "shardplanner.shard.count",
  "description": "Distribution of calculated shard counts",
  "baseUnit": null,
  "measurements": [
    {
      "statistic": "COUNT",
      "value": 5.0
    },
    {
      "statistic": "TOTAL",
      "value": 70.0
    },
    {
      "statistic": "MEAN",
      "value": 14.0
    }
  ],
  "availableTags": []
}
```

### Prometheus Format Response
```
# HELP shardplanner_shard_count Distribution of calculated shard counts
# TYPE shardplanner_shard_count summary
shardplanner_shard_count_count 5.0
shardplanner_shard_count_sum 70.0
shardplanner_shard_count_mean 14.0
```

## Browser Access

You can also access these endpoints in your browser:
- `http://localhost:8080/actuator/metrics`
- `http://localhost:8080/actuator/prometheus`
- `http://localhost:8080/actuator/health`

## Security Note

For production, consider:
1. Securing actuator endpoints with authentication
2. Using a different port for actuator endpoints
3. Limiting which endpoints are exposed

Example:
```properties
management.server.port=9090
management.endpoints.web.exposure.include=health,metrics
```
