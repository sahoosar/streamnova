# Finding LoadOperationEventLogger Event Logs

## ✅ Events ARE Being Logged!

Your `LoadOperationEventLogger` **is working correctly**. The events are being printed in your console/logs.

## Where to Find Event Logs

### In Your Terminal Output

Looking at your terminal file (`23.txt`), the EVENT logs appear at:

- **Line 64**: Initialization message
- **Lines 78-79**: `LOAD_OPERATION_STARTED` event
- **Lines 81-82**: `CONNECTION_ESTABLISHED` event  
- **Lines 85-86**: `STATISTICS_ESTIMATED` event
- **Lines 106-107**: `SHARD_CALCULATED` event
- **Lines 110-111**: `QUERY_BUILT` event
- **Lines 112-113**: `SCHEMA_DETECTED` event
- **Lines 117-118**: `LOAD_OPERATION_COMPLETED` event

### Example from Your Logs

```
Line 78: 21:43:15.503 INFO  [restartedMain] [app=StreamNova] c.d.s.util.LoadOperationEventLogger - EVENT: {"threadId": 16, "jobId": "job-caf13433-d00e-414d-b91b-24285f5e888a", "context": {"sourceType": "postgres", "jdbcUrl": "jdbc:postgresql://localhost:5432/marketdb", "tableName": "market_summary"}, "eventType": "LOAD_OPERATION_STARTED", "applicationId": "StreamNova-4f462998", "threadName": "restartedMain", "timestamp": "2026-01-23T21:43:15.503631Z"}

Line 79: EVENT: {"threadId": 16, "jobId": "job-caf13433-d00e-414d-b91b-24285f5e888a", "context": {"sourceType": "postgres", "jdbcUrl": "jdbc:postgresql://localhost:5432/marketdb", "tableName": "market_summary"}, "eventType": "LOAD_OPERATION_STARTED", "applicationId": "StreamNova-4f462998", "threadName": "restartedMain", "timestamp": "2026-01-23T21:43:15.503631Z"}
```

**Note:** Each event appears **twice**:
1. **Formatted log** (with timestamp, level, logger name) - Line 78
2. **Raw JSON** (from System.out) - Line 79

## Filtering for EVENT Logs Only

### Using grep

```bash
# Show only EVENT logs (raw JSON format)
grep "^EVENT:" your-log-file.txt

# Show only EVENT logs (formatted with logger)
grep "LoadOperationEventLogger.*EVENT:" your-log-file.txt

# Show all events for a specific job
grep "job-caf13433-d00e-414d-b91b-24285f5e888a" your-log-file.txt | grep "EVENT:"

# Show only specific event types
grep "EVENT:" your-log-file.txt | grep "LOAD_OPERATION_STARTED"
```

### Using tail with grep (Live Monitoring)

```bash
# Watch for new EVENT logs in real-time
tail -f your-log-file.txt | grep "EVENT:"

# Watch for specific event types
tail -f your-log-file.txt | grep "EVENT:" | grep "LOAD_OPERATION"
```

### Filtering by Event Type

```bash
# LOAD_OPERATION_STARTED events
grep "EVENT:" your-log-file.txt | grep "LOAD_OPERATION_STARTED"

# CONNECTION_ESTABLISHED events
grep "EVENT:" your-log-file.txt | grep "CONNECTION_ESTABLISHED"

# STATISTICS_ESTIMATED events
grep "EVENT:" your-log-file.txt | grep "STATISTICS_ESTIMATED"

# SHARD_CALCULATED events
grep "EVENT:" your-log-file.txt | grep "SHARD_CALCULATED"

# QUERY_BUILT events
grep "EVENT:" your-log-file.txt | grep "QUERY_BUILT"

# SCHEMA_DETECTED events
grep "EVENT:" your-log-file.txt | grep "SCHEMA_DETECTED"

# LOAD_OPERATION_COMPLETED events
grep "EVENT:" your-log-file.txt | grep "LOAD_OPERATION_COMPLETED"
```

## Understanding the Log Output

### Event Log Format

Each event appears in two formats:

1. **Formatted (SLF4J)**:
   ```
   21:43:15.503 INFO  [restartedMain] [app=StreamNova] c.d.s.util.LoadOperationEventLogger - EVENT: {...}
   ```

2. **Raw JSON (System.out)**:
   ```
   EVENT: {...}
   ```

### Event Structure

```json
{
  "eventType": "LOAD_OPERATION_STARTED",
  "timestamp": "2026-01-23T21:43:15.503631Z",
  "applicationId": "StreamNova-4f462998",
  "jobId": "job-caf13433-d00e-414d-b91b-24285f5e888a",
  "threadId": 16,
  "threadName": "restartedMain",
  "context": {
    "sourceType": "postgres",
    "tableName": "market_summary",
    "jdbcUrl": "jdbc:postgresql://localhost:5432/marketdb"
  }
}
```

## Why You Might Not See Events

### Issue: Looking at Wrong Lines

**Problem:** Lines 205-209 show `PerShard` logs, not EVENT logs.

**Solution:** EVENT logs appear earlier (around lines 64-118). Scroll up in your terminal/log file.

### Issue: Too Much Output

**Problem:** Events are mixed with other logs (PerShard, INFO, etc.)

**Solution:** Use grep to filter:
```bash
grep "^EVENT:" your-log-file.txt
```

### Issue: Events Scrolled Off Screen

**Problem:** Console buffer doesn't show old events.

**Solution:** 
- Check log files instead of console
- Use `less` or `tail` to view logs:
  ```bash
  tail -100 your-log-file.txt | grep "EVENT:"
  ```

## Quick Reference: All Event Types

Your logs show these event types:

1. ✅ `LOAD_OPERATION_STARTED` - When load operation begins
2. ✅ `CONNECTION_ESTABLISHED` - When DB connection is established
3. ✅ `STATISTICS_ESTIMATED` - After table statistics are estimated
4. ✅ `SHARD_CALCULATED` - After shard count is calculated
5. ✅ `QUERY_BUILT` - After SQL query is built
6. ✅ `SCHEMA_DETECTED` - After schema is detected
7. ✅ `LOAD_OPERATION_COMPLETED` - When load operation completes

## Summary

✅ **Events ARE working** - They're being logged successfully!

✅ **Location** - Events appear around lines 64-118 in your terminal output

✅ **Format** - Each event appears twice (formatted + raw JSON)

✅ **Filtering** - Use `grep "EVENT:"` to see only event logs

✅ **All event types** - All 7 event types are being logged correctly

The PerShard logs at lines 205-209 are **different logs** (from `LogPerShardFn`), not EVENT logs. The EVENT logs appear earlier in the output.
