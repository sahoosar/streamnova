# GlobalExceptionHandler Guide

## Overview

The `GlobalExceptionHandler` provides centralized exception handling for the application. It integrates with the `ErrorCategory` enum and `TransactionEventLogger` for comprehensive error tracking.

## What It Does

### ✅ Features

1. **Centralized Exception Handling**
   - Catches all unhandled exceptions
   - Provides consistent error responses
   - Works for REST API endpoints (if present)

2. **Error Categorization**
   - Uses `ErrorCategory.categorize()` automatically
   - Logs structured error events
   - Includes error category name and description

3. **Structured Logging**
   - Integrates with `TransactionEventLogger`
   - Logs error events with full context
   - Includes stack traces and root cause analysis

4. **HTTP Status Mapping**
   - Maps exceptions to appropriate HTTP status codes
   - Provides consistent error response format

## How It Works

### Exception Handling Flow

```
Exception Thrown
    ↓
@ExceptionHandler catches it
    ↓
ErrorCategory.categorize() → Categorizes the error
    ↓
TransactionEventLogger.logEvent() → Logs structured event
    ↓
ErrorResponse built → Returns structured response
```

### Current Handlers

| Exception Type | HTTP Status | Handler Method |
|---------------|-------------|----------------|
| `SQLException` | 500 (Internal Server Error) | `handleSqlException()` |
| `IllegalArgumentException` | 400 (Bad Request) | `handleValidationException()` |
| `IllegalStateException` | 400 (Bad Request) | `handleValidationException()` |
| `SocketTimeoutException` | 503 (Service Unavailable) | `handleNetworkException()` |
| `ConnectException` | 503 (Service Unavailable) | `handleNetworkException()` |
| `TimeoutException` | 408 (Request Timeout) | `handleTimeoutException()` |
| `OutOfMemoryError` | 507 (Insufficient Storage) | `handleResourceException()` |
| `FileSystemException` | 507 (Insufficient Storage) | `handleResourceException()` |
| `Exception` (catch-all) | 500 (Internal Server Error) | `handleGenericException()` |
| `Throwable` (all errors) | 500 (Internal Server Error) | `handleThrowable()` |

## When It's Used

### ✅ Works For:

1. **REST API Endpoints** (if you add controllers)
   - Automatically catches exceptions from `@RestController` methods
   - Returns JSON error responses

2. **Spring MVC Controllers**
   - Catches exceptions from `@Controller` methods
   - Works with `@RequestMapping`, `@GetMapping`, etc.

3. **Unhandled Exceptions**
   - Catches exceptions that escape other handlers
   - Provides fallback error handling

### ⚠️ Does NOT Work For:

1. **AOP-Intercepted Methods**
   - AOP (`TransactionEventAspect`) handles exceptions first
   - GlobalExceptionHandler only catches if AOP re-throws

2. **Batch Processing (Apache Beam)**
   - Beam workers run in separate processes
   - Exceptions in workers won't reach GlobalExceptionHandler

3. **Background Threads**
   - Exceptions in background threads need explicit handling
   - GlobalExceptionHandler only works for request-handling threads

## Integration with AOP

### How They Work Together

```
┌─────────────────────────────────────────────────┐
│  Method with @LogTransaction                    │
│  ┌───────────────────────────────────────────┐  │
│  │ TransactionEventAspect catches exception │  │
│  │ → Logs FAILED event with ErrorCategory   │  │
│  │ → Re-throws exception                    │  │
│  └───────────────────────────────────────────┘  │
│                    ↓                              │
│  ┌───────────────────────────────────────────┐  │
│  │ GlobalExceptionHandler catches exception  │  │
│  │ → Logs again (if REST endpoint)           │  │
│  │ → Returns error response                  │  │
│  └───────────────────────────────────────────┘  │
└─────────────────────────────────────────────────┘
```

**Note:** If the method is NOT a REST endpoint, GlobalExceptionHandler won't catch it (AOP handles it).

## Adding New Exception Handlers

### Step 1: Add Handler Method

**Location:** `GlobalExceptionHandler.java`

```java
/**
 * Handles your custom exception type.
 */
@ExceptionHandler(YourException.class)
@ResponseStatus(HttpStatus.YOUR_STATUS)
public ResponseEntity<ErrorResponse> handleYourException(YourException e) {
    ErrorCategory category = ErrorCategory.categorize(e);
    logError("YOUR_EXCEPTION", category, e);
    return ResponseEntity.status(HttpStatus.YOUR_STATUS)
            .body(buildErrorResponse(category, e, HttpStatus.YOUR_STATUS));
}
```

### Step 2: Ensure ErrorCategory Supports It

If your exception needs a new category:
1. Add category to `ErrorCategory` enum (see `HOW_TO_ADD_ERROR_CATEGORIES.md`)
2. The handler will automatically use it

### Example: Adding FileNotFoundException Handler

```java
@ExceptionHandler(java.io.FileNotFoundException.class)
@ResponseStatus(HttpStatus.NOT_FOUND)
public ResponseEntity<ErrorResponse> handleFileNotFoundException(
        java.io.FileNotFoundException e) {
    ErrorCategory category = ErrorCategory.categorize(e);
    logError("FILE_NOT_FOUND", category, e);
    return ResponseEntity.status(HttpStatus.NOT_FOUND)
            .body(buildErrorResponse(category, e, HttpStatus.NOT_FOUND));
}
```

## Error Response Format

### JSON Response Structure

```json
{
  "timestamp": "2026-01-24T10:53:30.123Z",
  "status": 500,
  "error": "Internal Server Error",
  "message": "Connection refused",
  "errorCategory": "CONNECTION_ERROR",
  "errorCategoryName": "Database connection error",
  "errorCategoryDescription": "Failed to establish or maintain database connection",
  "path": "/api/load",
  "details": {
    "exceptionType": "java.sql.SQLException",
    "rootCauseType": "java.net.ConnectException",
    "rootCauseMessage": "Connection refused: connect",
    "sqlState": "08001",
    "errorCode": 0
  }
}
```

## Usage Examples

### Example 1: REST Controller (Future Use)

```java
@RestController
@RequestMapping("/api")
public class LoadController {
    
    @PostMapping("/load")
    public ResponseEntity<String> startLoad(@RequestBody LoadRequest request) {
        // If exception occurs here, GlobalExceptionHandler catches it
        dataflowRunnerService.runPipeline();
        return ResponseEntity.ok("Load started");
    }
}
```

### Example 2: Current Usage (AOP Only)

```java
@LogTransaction(eventType = "POSTGRES_READ", ...)
public PCollection<Row> read(Pipeline pipeline, PipelineConfigSource config) {
    // If exception occurs:
    // 1. TransactionEventAspect catches it → logs FAILED event
    // 2. Re-throws exception
    // 3. GlobalExceptionHandler catches it (if REST endpoint)
    //    OR caller handles it (if not REST)
}
```

## Configuration

### Enable/Disable

The handler is automatically enabled when Spring Boot starts. No configuration needed.

### Customization

To customize behavior, modify:
- HTTP status codes in `@ResponseStatus` annotations
- Error response structure in `ErrorResponse` class
- Logging behavior in `logError()` method

## Benefits

### ✅ Advantages

1. **Centralized Handling** - All exceptions handled in one place
2. **Consistent Responses** - Same error format for all endpoints
3. **Error Categorization** - Automatic categorization using ErrorCategory
4. **Structured Logging** - Full error context in logs
5. **Easy to Extend** - Add new handlers easily

### ⚠️ Limitations

1. **Only for REST/MVC** - Doesn't catch exceptions in batch processing
2. **AOP Handles First** - AOP logs before GlobalExceptionHandler
3. **Background Threads** - Need explicit exception handling

## Best Practices

### ✅ DO:

1. **Use for REST endpoints** - Perfect for API error handling
2. **Add specific handlers** - Create handlers for specific exception types
3. **Use ErrorCategory** - Leverage automatic categorization
4. **Log structured events** - Use TransactionEventLogger integration

### ❌ DON'T:

1. **Don't rely on it for batch processing** - Use try-catch in batch code
2. **Don't duplicate AOP logging** - AOP already logs, handler adds REST response
3. **Don't ignore background threads** - Handle exceptions explicitly

## Testing

### Test Exception Handling

```java
@RestController
public class TestController {
    
    @GetMapping("/test-error")
    public String testError() {
        throw new IllegalArgumentException("Test error");
        // GlobalExceptionHandler will catch this and return:
        // {
        //   "status": 400,
        //   "errorCategory": "VALIDATION_ERROR",
        //   ...
        // }
    }
}
```

## Summary

**GlobalExceptionHandler is:**
- ✅ Ready to use for REST endpoints
- ✅ Integrated with ErrorCategory
- ✅ Integrated with TransactionEventLogger
- ✅ Easy to extend with new handlers

**Current Status:**
- ✅ Created and ready
- ⚠️ Will only work when REST controllers are added
- ✅ Works alongside AOP (AOP logs first, handler provides REST response)

**To Use:**
1. Add REST controllers (if needed)
2. Exceptions will be automatically handled
3. Add specific handlers for custom exceptions (see examples above)
