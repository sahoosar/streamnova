# Event-Based Logging Guide

## Overview

Event-based logging has been implemented to track all data loading operations with structured, parseable event logs. This provides a comprehensive audit trail of loading operations, making it easy to monitor, debug, and analyze data loading activities.

## Features

- ✅ **Structured Event Logs**: JSON-like format for easy parsing
- ✅ **Comprehensive Coverage**: Events logged at all key milestones
- ✅ **Contextual Information**: Each event includes relevant metadata
- ✅ **Job Tracking**: Events are tagged with jobId from MDC
- ✅ **Thread Information**: Includes thread ID and name for debugging
- ✅ **Timestamps**: ISO 8601 formatted timestamps
- ✅ **Error Tracking**: Failed operations are logged with error details

## Event Types

### 1. `LOAD_OPERATION_STARTED`
**When:** At the start of a data loading operation

**Context:**
- `sourceType`: Type of source (e.g., "postgres")
- `tableName`: Name of the table being loaded
- `jdbcUrl`: JDBC URL (sanitized, password removed)

**Example:**
```json
{
  "eventType": "LOAD_OPERATION_STARTED",
  "timestamp": "2026-01-23T21:25:15.123Z",
  "jobId": "job-abc123",
  "threadId": 1,
  "threadName": "main",
  "context": {
    "sourceType": "postgres",
    "tableName": "public.users",
    "jdbcUrl": "jdbc:postgresql://localhost:5432/mydb"
  }
}
```

### 2. `LOAD_OPERATION_COMPLETED`
**When:** When a data loading operation completes successfully

**Context:**
- `sourceType`: Type of source
- `tableName`: Name of the table
- `durationMs`: Duration in milliseconds
- `durationSeconds`: Duration in seconds (for readability)
- `estimatedRowCount`: Estimated number of rows (if available)
- `shardCount`: Number of shards used

**Example:**
```json
{
  "eventType": "LOAD_OPERATION_COMPLETED",
  "timestamp": "2026-01-23T21:25:20.456Z",
  "jobId": "job-abc123",
  "threadId": 1,
  "threadName": "main",
  "context": {
    "sourceType": "postgres",
    "tableName": "public.users",
    "durationMs": 5333,
    "durationSeconds": 5.333,
    "estimatedRowCount": 1000000,
    "shardCount": 16
  }
}
```

### 3. `LOAD_OPERATION_FAILED`
**When:** When a data loading operation fails

**Context:**
- `sourceType`: Type of source
- `tableName`: Name of the table
- `errorMessage`: Error message
- `errorType`: Exception class name
- `durationMs`: Duration before failure
- `durationSeconds`: Duration in seconds

**Example:**
```json
{
  "eventType": "LOAD_OPERATION_FAILED",
  "timestamp": "2026-01-23T21:25:18.789Z",
  "jobId": "job-abc123",
  "threadId": 1,
  "threadName": "main",
  "context": {
    "sourceType": "postgres",
    "tableName": "public.users",
    "errorMessage": "Connection refused",
    "errorType": "SQLException",
    "durationMs": 2000,
    "durationSeconds": 2.0
  }
}
```

### 4. `STATISTICS_ESTIMATED`
**When:** After table statistics are estimated

**Context:**
- `tableName`: Name of the table
- `estimatedRowCount`: Estimated number of rows (if available)
- `averageRowSizeBytes`: Average row size in bytes (if available)
- `durationMs`: Time taken to estimate statistics

**Example:**
```json
{
  "eventType": "STATISTICS_ESTIMATED",
  "timestamp": "2026-01-23T21:25:15.234Z",
  "jobId": "job-abc123",
  "threadId": 1,
  "threadName": "main",
  "context": {
    "tableName": "public.users",
    "estimatedRowCount": 1000000,
    "averageRowSizeBytes": 200,
    "durationMs": 150
  }
}
```

### 5. `SHARD_CALCULATED`
**When:** After optimal shard count is calculated

**Context:**
- `tableName`: Name of the table
- `shardCount`: Calculated number of shards
- `strategy`: Strategy used (e.g., "optimal")
- `virtualCpus`: Number of virtual CPUs (if available)
- `workerCount`: Number of workers (if available)
- `durationMs`: Time taken to calculate shards

**Example:**
```json
{
  "eventType": "SHARD_CALCULATED",
  "timestamp": "2026-01-23T21:25:15.500Z",
  "jobId": "job-abc123",
  "threadId": 1,
  "threadName": "main",
  "context": {
    "tableName": "public.users",
    "shardCount": 16,
    "strategy": "optimal",
    "virtualCpus": 14,
    "workerCount": 4,
    "durationMs": 50
  }
}
```

### 6. `SCHEMA_DETECTED`
**When:** After table schema is detected

**Context:**
- `tableName`: Name of the table
- `fieldCount`: Number of fields in the schema
- `durationMs`: Time taken to detect schema

**Example:**
```json
{
  "eventType": "SCHEMA_DETECTED",
  "timestamp": "2026-01-23T21:25:16.100Z",
  "jobId": "job-abc123",
  "threadId": 1,
  "threadName": "main",
  "context": {
    "tableName": "public.users",
    "fieldCount": 10,
    "durationMs": 200
  }
}
```

### 7. `QUERY_BUILT`
**When:** After SQL query is built

**Context:**
- `tableName`: Name of the table
- `shardExpression`: Shard expression used
- `orderByColumns`: Order by columns (comma-separated)
- `fetchSize`: Fetch size configured
- `shardCount`: Number of shards

**Example:**
```json
{
  "eventType": "QUERY_BUILT",
  "timestamp": "2026-01-23T21:25:16.300Z",
  "jobId": "job-abc123",
  "threadId": 1,
  "threadName": "main",
  "context": {
    "tableName": "public.users",
    "shardExpression": "MOD(ABS(HASHTEXT(id)), 16)",
    "orderByColumns": "id, created_at",
    "fetchSize": 1000,
    "shardCount": 16
  }
}
```

### 8. `CONNECTION_ESTABLISHED`
**When:** When database connection is established

**Context:**
- `jdbcUrl`: JDBC URL (sanitized)
- `username`: Database username
- `poolSize`: Connection pool size

**Example:**
```json
{
  "eventType": "CONNECTION_ESTABLISHED",
  "timestamp": "2026-01-23T21:25:15.100Z",
  "jobId": "job-abc123",
  "threadId": 1,
  "threadName": "main",
  "context": {
    "jdbcUrl": "jdbc:postgresql://localhost:5432/mydb",
    "username": "myuser",
    "poolSize": 10
  }
}
```

### 9. `VALIDATION_COMPLETED`
**When:** After validation operations complete

**Context:**
- `tableName`: Name of the table
- `validationType`: Type of validation
- `passed`: Whether validation passed

**Example:**
```json
{
  "eventType": "VALIDATION_COMPLETED",
  "timestamp": "2026-01-23T21:25:15.050Z",
  "jobId": "job-abc123",
  "threadId": 1,
  "threadName": "main",
  "context": {
    "tableName": "public.users",
    "validationType": "table_name",
    "passed": true
  }
}
```

## Log Format

All events are logged with the prefix `EVENT:` followed by a JSON-like structure:

```
EVENT: {"eventType": "...", "timestamp": "...", "jobId": "...", ...}
```

## Usage

### Automatic Integration

Event logging is **automatically integrated** into:
- `DataflowRunnerService`: Logs load operation start/failure
- `PostgresHandler`: Logs all handler-level events

No additional code is required - events are logged automatically during normal operations.

### Manual Event Logging

If you need to log custom events, inject `LoadOperationEventLogger`:

```java
@Autowired
private LoadOperationEventLogger eventLogger;

public void myMethod() {
    Map<String, Object> context = new HashMap<>();
    context.put("customField", "customValue");
    eventLogger.logEvent("CUSTOM_EVENT", context);
}
```

## Log Parsing

### Using grep

Filter events by type:
```bash
grep "EVENT:" application.log | grep "LOAD_OPERATION_STARTED"
```

Filter by jobId:
```bash
grep "EVENT:" application.log | grep "job-abc123"
```

### Using jq (if logs are in JSON format)

```bash
cat application.log | grep "EVENT:" | jq -r '.context.tableName'
```

### Using Python

```python
import re
import json

log_line = 'EVENT: {"eventType": "LOAD_OPERATION_STARTED", ...}'
match = re.search(r'EVENT: (.+)', log_line)
if match:
    event = json.loads(match.group(1))
    print(f"Event: {event['eventType']}")
    print(f"Table: {event['context']['tableName']}")
```

## Security Considerations

- ✅ **JDBC URLs are sanitized**: Passwords are removed from JDBC URLs before logging
- ✅ **No sensitive data**: Passwords and other sensitive information are never logged
- ✅ **Table names validated**: Table names are validated before logging

## Monitoring and Alerting

### Example: Monitor Failed Loads

```bash
# Count failed loads in last hour
grep "EVENT:" application.log | grep "LOAD_OPERATION_FAILED" | wc -l
```

### Example: Track Load Duration

```bash
# Extract duration for completed loads
grep "EVENT:" application.log | grep "LOAD_OPERATION_COMPLETED" | \
  grep -oP '"durationSeconds":\s*\K[0-9.]+'
```

### Example: Find Slow Operations

```bash
# Find loads taking more than 10 seconds
grep "EVENT:" application.log | grep "LOAD_OPERATION_COMPLETED" | \
  awk -F'"durationSeconds":' '{print $2}' | awk -F',' '{if ($1 > 10) print}'
```

## Integration with Log Aggregation

### ELK Stack (Elasticsearch, Logstash, Kibana)

Configure Logstash to parse events:
```ruby
filter {
  if [message] =~ /^EVENT:/ {
    grok {
      match => { "message" => "EVENT: %{GREEDYDATA:event_json}" }
    }
    json {
      source => "event_json"
    }
  }
}
```

### Splunk

Use regex extraction:
```
EVENT:\s+(?<event_json>.*)
```

Then parse JSON fields in Splunk.

### CloudWatch Logs Insights

Query example:
```
fields @timestamp, eventType, context.tableName, context.durationMs
| filter @message like /EVENT:/
| filter eventType = "LOAD_OPERATION_COMPLETED"
| stats avg(context.durationMs) by context.tableName
```

## Best Practices

1. **Monitor Event Logs**: Set up alerts for `LOAD_OPERATION_FAILED` events
2. **Track Performance**: Monitor `durationMs` in completed events
3. **Analyze Patterns**: Use event logs to identify slow tables or operations
4. **Audit Trail**: Use events for compliance and audit purposes
5. **Debugging**: Use jobId to trace all events for a specific operation

## Troubleshooting

### Events Not Appearing

1. Check that `LoadOperationEventLogger` is properly autowired
2. Verify MDC has `jobId` set (required for event correlation)
3. Check log level - events are logged at INFO level

### Parsing Issues

1. Ensure log format matches expected JSON-like structure
2. Check for special characters in table names that might break parsing
3. Verify timestamp format is ISO 8601

## Example Log Output

```
2026-01-23 21:25:15.123 INFO  [main] c.d.s.util.LoadOperationEventLogger - EVENT: {"eventType": "LOAD_OPERATION_STARTED", "timestamp": "2026-01-23T21:25:15.123Z", "jobId": "job-abc123", "threadId": 1, "threadName": "main", "context": {"sourceType": "postgres", "tableName": "public.users", "jdbcUrl": "jdbc:postgresql://localhost:5432/mydb"}}
2026-01-23 21:25:15.234 INFO  [main] c.d.s.util.LoadOperationEventLogger - EVENT: {"eventType": "STATISTICS_ESTIMATED", "timestamp": "2026-01-23T21:25:15.234Z", "jobId": "job-abc123", "threadId": 1, "threadName": "main", "context": {"tableName": "public.users", "estimatedRowCount": 1000000, "averageRowSizeBytes": 200, "durationMs": 150}}
2026-01-23 21:25:15.500 INFO  [main] c.d.s.util.LoadOperationEventLogger - EVENT: {"eventType": "SHARD_CALCULATED", "timestamp": "2026-01-23T21:25:15.500Z", "jobId": "job-abc123", "threadId": 1, "threadName": "main", "context": {"tableName": "public.users", "shardCount": 16, "strategy": "optimal", "virtualCpus": 14, "workerCount": 4, "durationMs": 50}}
2026-01-23 21:25:20.456 INFO  [main] c.d.s.util.LoadOperationEventLogger - EVENT: {"eventType": "LOAD_OPERATION_COMPLETED", "timestamp": "2026-01-23T21:25:20.456Z", "jobId": "job-abc123", "threadId": 1, "threadName": "main", "context": {"sourceType": "postgres", "tableName": "public.users", "durationMs": 5333, "durationSeconds": 5.333, "estimatedRowCount": 1000000, "shardCount": 16}}
```

## Summary

Event-based logging provides a comprehensive, structured way to track all data loading operations. All events are automatically logged during normal operations, providing:

- ✅ Complete audit trail
- ✅ Performance monitoring
- ✅ Error tracking
- ✅ Easy integration with log aggregation tools
- ✅ Security (sensitive data sanitized)

No additional configuration is required - events are logged automatically!
