# Error Tracking & Troubleshooting Guide

## Overview

Enhanced error tracking has been implemented to make troubleshooting easier. All errors are now logged with comprehensive context, stack traces, error categories, and correlation information.

## Enhanced Error Event Structure

### Complete Error Event Format

```json
{
  "eventType": "LOAD_OPERATION_FAILED",
  "timestamp": "2026-01-23T21:30:33.123Z",
  "applicationId": "StreamNova-a1b2c3d4",
  "jobId": "job-abc123",
  "threadId": 1,
  "threadName": "main",
  "context": {
    "sourceType": "postgres",
    "tableName": "public.users",
    "errorMessage": "Connection refused",
    "errorType": "SQLException",
    "errorCategory": "CONNECTION_ERROR",
    "operationContext": "postgres_read",
    "durationMs": 2000,
    "durationSeconds": 2.0,
    "rootCauseType": "ConnectException",
    "rootCauseMessage": "Connection refused: connect",
    "stackTraceSummary": "SQLException: Connection refused | at com.di.streamnova... | at java.sql... | ... (15 more lines)",
    "sqlState": "08001",
    "errorCode": 0,
    "hasChainedExceptions": false
  }
}
```

## Error Tracking Features

### 1. **Error Categories** ✅

Errors are automatically categorized for easy filtering:

| Category | Description | Examples |
|----------|-------------|----------|
| `CONNECTION_ERROR` | Database connection issues | Connection refused, timeout, network errors |
| `DATABASE_ERROR` | General database errors | SQL exceptions, constraint violations |
| `VALIDATION_ERROR` | Input validation failures | Invalid table name, invalid parameters |
| `CONFIGURATION_ERROR` | Configuration issues | Missing config, invalid settings |
| `NETWORK_ERROR` | Network-related errors | Socket timeout, connection refused |
| `RESOURCE_ERROR` | Resource exhaustion | Out of memory, file system errors |
| `APPLICATION_ERROR` | Application logic errors | Null pointer, illegal state |
| `CONSTRAINT_VIOLATION` | Database constraint violations | Primary key violations, foreign key errors |
| `SQL_SYNTAX_ERROR` | SQL syntax errors | Invalid SQL statements |
| `TRANSACTION_ROLLBACK` | Transaction failures | Deadlock, rollback errors |
| `PERMISSION_ERROR` | Permission/access errors | Access denied, insufficient privileges |

**Usage:**
```bash
# Filter by error category
grep "EVENT:" logs/*.log | grep "CONNECTION_ERROR"
```

### 2. **Stack Trace Summary** ✅

First 5 lines of stack trace included in event for quick troubleshooting:

```json
"stackTraceSummary": "SQLException: Connection refused | at com.di.streamnova.handler.impl.PostgresHandler.read(PostgresHandler.java:175) | at java.sql.DriverManager.getConnection(DriverManager.java:123) | ... (15 more lines)"
```

**Benefits:**
- Quick identification of error location
- No need to search through full logs
- Easy correlation with code

### 3. **Root Cause Information** ✅

Automatic root cause detection:

```json
"rootCauseType": "ConnectException",
"rootCauseMessage": "Connection refused: connect"
```

**Benefits:**
- Identifies underlying issues
- Helps with root cause analysis
- Distinguishes symptoms from causes

### 4. **Operation Context** ✅

Tracks what operation was being performed when error occurred:

```json
"operationContext": "postgres_read"
```

**Possible Values:**
- `postgres_read`: Main read operation
- `load_operation_start`: Starting load operation
- `schema_detection`: Schema detection phase
- `statistics_estimation`: Statistics estimation phase
- `shard_calculation`: Shard calculation phase
- `query_building`: Query building phase
- `connection_establishment`: Connection setup

**Usage:**
```bash
# Find all schema detection errors
grep "EVENT:" logs/*.log | grep "schema_detection"
```

### 5. **SQL Error Details** ✅

For SQL exceptions, includes SQL-specific information:

```json
"sqlState": "08001",
"errorCode": 0,
"hasChainedExceptions": false
```

**SQL State Codes:**
- `08xxx`: Connection exceptions
- `23xxx`: Constraint violations
- `42xxx`: SQL syntax errors
- `40xxx`: Transaction rollback

**Usage:**
```bash
# Find all connection errors (SQL state 08xxx)
grep "EVENT:" logs/*.log | grep '"sqlState": "08'
```

### 6. **Error Correlation** ✅

All errors are correlated with:
- `applicationId`: Which application instance
- `jobId`: Which job/operation
- `threadId`: Which thread
- `timestamp`: When it occurred

**Usage:**
```bash
# Find all errors for a specific job
grep "EVENT:" logs/*.log | grep "job-abc123" | grep "LOAD_OPERATION_FAILED"

# Find all errors from a specific application
grep "EVENT:" logs/*.log | grep "StreamNova-a1b2c3d4" | grep "LOAD_OPERATION_FAILED"
```

## Troubleshooting Workflows

### Workflow 1: Find All Errors for a Job

```bash
# Step 1: Find the job ID from start event
grep "EVENT:" logs/*.log | grep "LOAD_OPERATION_STARTED" | grep "table_name"

# Step 2: Find all events for that job
JOB_ID="job-abc123"
grep "EVENT:" logs/*.log | grep "$JOB_ID"

# Step 3: Filter for errors only
grep "EVENT:" logs/*.log | grep "$JOB_ID" | grep "LOAD_OPERATION_FAILED"
```

### Workflow 2: Find Connection Errors

```bash
# Find all connection errors
grep "EVENT:" logs/*.log | grep "CONNECTION_ERROR"

# Find connection errors for specific table
grep "EVENT:" logs/*.log | grep "CONNECTION_ERROR" | grep "public.users"

# Find connection errors in last hour
grep "EVENT:" logs/*.log | grep "CONNECTION_ERROR" | grep "2026-01-23T21"
```

### Workflow 3: Analyze Error Patterns

```bash
# Count errors by category
grep "EVENT:" logs/*.log | grep "LOAD_OPERATION_FAILED" | \
  grep -oP '"errorCategory":\s*"\K[^"]+' | sort | uniq -c

# Count errors by operation context
grep "EVENT:" logs/*.log | grep "LOAD_OPERATION_FAILED" | \
  grep -oP '"operationContext":\s*"\K[^"]+' | sort | uniq -c

# Find most common error types
grep "EVENT:" logs/*.log | grep "LOAD_OPERATION_FAILED" | \
  grep -oP '"errorType":\s*"\K[^"]+' | sort | uniq -c | sort -rn
```

### Workflow 4: Root Cause Analysis

```bash
# Find errors with root cause different from error type
grep "EVENT:" logs/*.log | grep "LOAD_OPERATION_FAILED" | \
  grep -v '"rootCauseType":\s*null'

# Find specific root cause
grep "EVENT:" logs/*.log | grep "LOAD_OPERATION_FAILED" | \
  grep "ConnectException"
```

### Workflow 5: Time-Based Analysis

```bash
# Find errors in specific time range
grep "EVENT:" logs/*.log | grep "LOAD_OPERATION_FAILED" | \
  grep "2026-01-23T21:3[0-9]"

# Find errors with duration > 5 seconds (slow failures)
grep "EVENT:" logs/*.log | grep "LOAD_OPERATION_FAILED" | \
  awk -F'"durationSeconds":' '{print $2}' | awk -F',' '{if ($1 > 5) print}'
```

## Log Aggregation Queries

### ELK Stack (Elasticsearch, Logstash, Kibana)

**Kibana Queries:**

```
# All connection errors
event_json.context.errorCategory: "CONNECTION_ERROR"

# Errors for specific table
event_json.context.tableName: "public.users" AND event_json.eventType: "LOAD_OPERATION_FAILED"

# Errors in last hour
event_json.timestamp: [now-1h TO now] AND event_json.eventType: "LOAD_OPERATION_FAILED"

# Errors by application
event_json.applicationId: "StreamNova-a1b2c3d4" AND event_json.eventType: "LOAD_OPERATION_FAILED"

# Errors with stack trace containing specific class
event_json.context.stackTraceSummary: "*PostgresHandler*"
```

### Splunk

**Search Queries:**

```
# All connection errors
index=main "EVENT:" "CONNECTION_ERROR"

# Errors for specific job
index=main "EVENT:" "job-abc123" "LOAD_OPERATION_FAILED"

# Errors by category (stats)
index=main "EVENT:" "LOAD_OPERATION_FAILED" 
| rex field=_raw "errorCategory\":\s*\"(?<category>[^\"]+)"
| stats count by category

# Errors with duration > 5 seconds
index=main "EVENT:" "LOAD_OPERATION_FAILED"
| rex field=_raw "durationSeconds\":\s*(?<duration>[0-9.]+)"
| where duration > 5
```

### CloudWatch Logs Insights

**Query:**

```
fields @timestamp, eventType, applicationId, jobId, 
       context.errorCategory, context.errorType, 
       context.operationContext, context.durationSeconds
| filter @message like /EVENT:/
| filter eventType = "LOAD_OPERATION_FAILED"
| filter context.errorCategory = "CONNECTION_ERROR"
| stats count() by context.errorType
```

## Error Alerting Examples

### Alert: High Error Rate

```bash
# Count errors in last 5 minutes
ERROR_COUNT=$(grep "EVENT:" logs/*.log | \
  grep "$(date -u +%Y-%m-%dT%H:%M)" | \
  grep "LOAD_OPERATION_FAILED" | wc -l)

if [ "$ERROR_COUNT" -gt 10 ]; then
  echo "ALERT: High error rate detected: $ERROR_COUNT errors"
fi
```

### Alert: Connection Errors

```bash
# Check for connection errors in last minute
CONNECTION_ERRORS=$(grep "EVENT:" logs/*.log | \
  grep "$(date -u +%Y-%m-%dT%H:%M)" | \
  grep "CONNECTION_ERROR" | wc -l)

if [ "$CONNECTION_ERRORS" -gt 0 ]; then
  echo "ALERT: Connection errors detected: $CONNECTION_ERRORS"
fi
```

### Alert: Specific Error Type

```bash
# Check for specific error
SQL_ERRORS=$(grep "EVENT:" logs/*.log | \
  grep "$(date -u +%Y-%m-%dT%H:%M)" | \
  grep '"errorType": "SQLException"' | wc -l)

if [ "$SQL_ERRORS" -gt 5 ]; then
  echo "ALERT: Multiple SQL exceptions detected: $SQL_ERRORS"
fi
```

## Best Practices

### 1. **Monitor Error Categories**

Set up alerts for each error category:
- `CONNECTION_ERROR`: Database connectivity issues
- `VALIDATION_ERROR`: Configuration/input issues
- `RESOURCE_ERROR`: Resource exhaustion

### 2. **Track Error Trends**

Monitor error rates over time:
```bash
# Daily error count
grep "EVENT:" logs/*.log | grep "LOAD_OPERATION_FAILED" | \
  grep -oP '"timestamp":\s*"\K[^"]+' | cut -d'T' -f1 | sort | uniq -c
```

### 3. **Correlate with Operations**

Link errors to specific operations:
```bash
# Find all errors during schema detection
grep "EVENT:" logs/*.log | grep "schema_detection" | grep "LOAD_OPERATION_FAILED"
```

### 4. **Use Root Cause for Analysis**

Focus on root causes, not symptoms:
```bash
# Find root causes
grep "EVENT:" logs/*.log | grep "LOAD_OPERATION_FAILED" | \
  grep -oP '"rootCauseType":\s*"\K[^"]+' | sort | uniq -c
```

### 5. **Monitor Error Duration**

Track how long operations take before failing:
```bash
# Find slow failures (> 10 seconds)
grep "EVENT:" logs/*.log | grep "LOAD_OPERATION_FAILED" | \
  awk -F'"durationSeconds":' '{print $2}' | awk -F',' '{if ($1 > 10) print}'
```

## Example Error Scenarios

### Scenario 1: Connection Timeout

**Event:**
```json
{
  "eventType": "LOAD_OPERATION_FAILED",
  "context": {
    "errorCategory": "CONNECTION_ERROR",
    "errorType": "SQLException",
    "errorMessage": "Connection timed out",
    "operationContext": "postgres_read",
    "sqlState": "08001",
    "rootCauseType": "SocketTimeoutException",
    "rootCauseMessage": "Read timed out"
  }
}
```

**Troubleshooting:**
1. Check database connectivity
2. Verify network configuration
3. Check connection pool settings
4. Review timeout configurations

### Scenario 2: Invalid Table Name

**Event:**
```json
{
  "eventType": "LOAD_OPERATION_FAILED",
  "context": {
    "errorCategory": "VALIDATION_ERROR",
    "errorType": "IllegalArgumentException",
    "errorMessage": "Invalid table name format",
    "operationContext": "load_operation_start",
    "rootCauseType": "IllegalArgumentException"
  }
}
```

**Troubleshooting:**
1. Verify table name format
2. Check configuration file
3. Validate table exists in database

### Scenario 3: Schema Detection Failure

**Event:**
```json
{
  "eventType": "LOAD_OPERATION_FAILED",
  "context": {
    "errorCategory": "DATABASE_ERROR",
    "errorType": "SQLException",
    "errorMessage": "Table does not exist",
    "operationContext": "schema_detection",
    "sqlState": "42P01",
    "rootCauseType": "SQLException"
  }
}
```

**Troubleshooting:**
1. Verify table exists
2. Check schema name
3. Verify database permissions
4. Review SQL query

## Summary

### ✅ Enhanced Error Tracking Features

1. **Error Categories**: Automatic categorization for easy filtering
2. **Stack Trace Summary**: First 5 lines for quick troubleshooting
3. **Root Cause Detection**: Automatic root cause identification
4. **Operation Context**: Tracks what operation was being performed
5. **SQL Error Details**: SQL state codes and error codes for database errors
6. **Error Correlation**: Links errors to applications, jobs, and threads
7. **Duration Tracking**: Tracks how long operations take before failing

### ✅ Easy Troubleshooting

- **Filter by category**: Find all connection errors
- **Filter by operation**: Find schema detection errors
- **Filter by job**: Find all errors for a specific job
- **Root cause analysis**: Identify underlying issues
- **Time-based analysis**: Find errors in specific time ranges
- **Pattern analysis**: Identify common error patterns

### ✅ Production Ready

- Comprehensive error context
- Easy log aggregation integration
- Alert-ready structure
- Root cause analysis support
- Performance tracking (duration)

**No additional configuration needed** - enhanced error tracking works automatically! 🎉
