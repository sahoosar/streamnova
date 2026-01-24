# LoadOperationEventLogger Console Visibility - Troubleshooting

## Issue: Events Not Visible in Console

If you're not seeing `LoadOperationEventLogger` messages in the console, here are the fixes applied and how to verify:

## ✅ Fixes Applied

### 1. **Explicit Logger Configuration**

Added explicit logger configuration in `logback-spring.xml`:

```xml
<logger name="com.di.streamnova.util.LoadOperationEventLogger" level="INFO" additivity="true"/>
```

### 2. **System.out Fallback**

Added `System.out.println` as a fallback to ensure events are always visible:

```java
log.info("EVENT: {}", eventJson);
System.out.println("EVENT: " + eventJson); // Fallback for console visibility
```

## How to Verify

### Step 1: Check Logger Initialization

When the application starts, you should see:

```
INFO  [main] c.d.s.util.LoadOperationEventLogger - LoadOperationEventLogger initialized with applicationId: StreamNova-xxxxx
```

If you see this, the logger is working.

### Step 2: Check Event Logging

When a load operation runs, you should see events like:

```
INFO  [main] c.d.s.util.LoadOperationEventLogger - EVENT: {"eventType": "LOAD_OPERATION_STARTED", ...}
EVENT: {"eventType": "LOAD_OPERATION_STARTED", ...}
```

**Note:** You'll see the event **twice**:
1. Once from SLF4J logger (formatted with timestamp, level, etc.)
2. Once from System.out (raw event JSON)

### Step 3: Verify Events Are Being Called

Check if events are actually being triggered:

1. **Check if `runPipeline()` is being called:**
   ```bash
   # Look for pipeline execution logs
   grep "Starting pipeline" logs/*.log
   ```

2. **Check if PostgresHandler.read() is being called:**
   ```bash
   # Look for PostgresHandler logs
   grep "Postgres read plan" logs/*.log
   ```

3. **Check for initialization message:**
   ```bash
   # Look for LoadOperationEventLogger initialization
   grep "LoadOperationEventLogger initialized" logs/*.log
   ```

## Common Issues & Solutions

### Issue 1: Logger Not Initialized

**Symptom:** No initialization message

**Solution:**
- Verify `LoadOperationEventLogger` is a `@Component`
- Check Spring component scanning includes `com.di.streamnova`
- Verify the application is actually starting

### Issue 2: Events Not Triggered

**Symptom:** Initialization message appears, but no events

**Solution:**
- Verify `runPipeline()` is being called
- Check if `PostgresHandler.read()` is being called
- Verify MDC has `jobId` set (events will show `jobId: "unknown" if not set)

### Issue 3: Log Level Too High

**Symptom:** Events logged but not visible

**Solution:**
- Check `application.properties`: `logging.level.com.di.streamnova=INFO`
- Check `logback-spring.xml`: logger level is INFO
- Verify root logger level is INFO or lower

### Issue 4: Logger Name Mismatch

**Symptom:** Events logged but filtered out

**Solution:**
- Verify logger name in logback matches: `com.di.streamnova.util.LoadOperationEventLogger`
- Check for conflicting logger configurations

## Testing the Logger

### Manual Test

Add this to your code temporarily to test:

```java
@Autowired
private LoadOperationEventLogger eventLogger;

public void testLogger() {
    eventLogger.logLoadStarted("test", "test_table", "jdbc:test://localhost/test");
}
```

You should see:
```
INFO  [main] c.d.s.util.LoadOperationEventLogger - EVENT: {"eventType": "LOAD_OPERATION_STARTED", ...}
EVENT: {"eventType": "LOAD_OPERATION_STARTED", ...}
```

### Verify in Application

1. **Start the application**
2. **Trigger a load operation** (call `runPipeline()`)
3. **Check console output** for:
   - Initialization message
   - `EVENT:` messages (both formatted and raw)

## Expected Console Output

### On Application Start:
```
21:41:47.123 INFO  [main] c.d.s.util.LoadOperationEventLogger - LoadOperationEventLogger initialized with applicationId: StreamNova-a1b2c3d4
```

### During Load Operation:
```
21:41:48.234 INFO  [main] c.d.s.util.LoadOperationEventLogger - EVENT: {"eventType": "LOAD_OPERATION_STARTED", "timestamp": "2026-01-23T21:41:48.234Z", "applicationId": "StreamNova-a1b2c3d4", "jobId": "job-abc123", "threadId": 1, "threadName": "main", "context": {"sourceType": "postgres", "tableName": "public.users", "jdbcUrl": "jdbc:postgresql://localhost:5432/mydb"}}
EVENT: {"eventType": "LOAD_OPERATION_STARTED", "timestamp": "2026-01-23T21:41:48.234Z", "applicationId": "StreamNova-a1b2c3d4", "jobId": "job-abc123", "threadId": 1, "threadName": "main", "context": {"sourceType": "postgres", "tableName": "public.users", "jdbcUrl": "jdbc:postgresql://localhost:5432/mydb"}}
```

## Removing System.out (Optional)

Once you've verified logging works, you can remove the `System.out.println` line:

```java
// Remove this line after verification
System.out.println("EVENT: " + eventJson);
```

The SLF4J logger should be sufficient for production use.

## Still Not Working?

If events still don't appear:

1. **Check if the application is actually running:**
   ```bash
   # Look for any application logs
   tail -f logs/*.log
   ```

2. **Verify Spring context:**
   - Check if `LoadOperationEventLogger` is being autowired
   - Verify no errors during Spring initialization

3. **Check for log filtering:**
   - Verify no log filters are suppressing events
   - Check for custom log appenders that might filter

4. **Enable debug logging:**
   ```properties
   logging.level.com.di.streamnova.util.LoadOperationEventLogger=DEBUG
   ```

5. **Test with a simple logger:**
   ```java
   @Component
   public class TestLogger {
       private static final Logger log = LoggerFactory.getLogger(TestLogger.class);
       
       @PostConstruct
       public void test() {
           log.info("TEST: LoadOperationEventLogger test");
       }
   }
   ```

## Summary

✅ **Fixes Applied:**
1. Explicit logger configuration in logback-spring.xml
2. System.out fallback for immediate visibility
3. Verification steps documented

✅ **Expected Behavior:**
- Events appear in console (both formatted and raw)
- Initialization message on startup
- Events logged during load operations

✅ **If Still Not Visible:**
- Check if application is running
- Verify events are being triggered
- Check log levels
- Enable debug logging
