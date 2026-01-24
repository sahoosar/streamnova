# How to Add New Error Categories

## Quick Guide

This guide explains how to add new error categories to the `ErrorCategory` enum for better error tracking and monitoring.

## Step-by-Step Instructions

### Step 1: Add Enum Constant

**Location:** `src/main/java/com/di/streamnova/aspect/ErrorCategory.java`

Add your new category **BEFORE** the `UNKNOWN` constant (which must be last):

```java
/**
 * Your new error category description.
 */
YOUR_NEW_ERROR("Display Name", "Detailed description of when this error occurs"),

// ... other categories ...

UNKNOWN("Unknown error", "Unclassified or unknown error type");
```

**Example:**
```java
/**
 * File system errors (e.g., file not found, permission denied).
 */
FILE_SYSTEM_ERROR("File system error", "File system operation failure"),

UNKNOWN("Unknown error", "Unclassified or unknown error type");
```

### Step 2: Add Categorization Logic

**Location:** `ErrorCategory.categorize()` method

Add your check **BEFORE** `APPLICATION_ERROR` (which is the default fallback):

```java
// Find this section in categorize() method:
// ========== ADD NEW ERROR CATEGORIES HERE ==========

// Add your check:
if (isYourNewError(exception)) {
    return YOUR_NEW_ERROR;
}

// ====================================================

// Default to application error (must be last)
return APPLICATION_ERROR;
```

**Example:**
```java
// File system errors
if (isFileSystemError(exception)) {
    return FILE_SYSTEM_ERROR;
}

// Default to application error (must be last)
return APPLICATION_ERROR;
```

### Step 3: Implement Helper Method

**Location:** At the end of `ErrorCategory` class, in the section marked:
```java
// ========== ADD NEW ERROR CATEGORY HELPER METHODS HERE ==========
```

**Template:**
```java
/**
 * Checks if the exception is a [your error type] error.
 */
private static boolean isYourNewError(Throwable exception) {
    // Option 1: Check exception type
    if (exception instanceof YourExceptionClass) {
        return true;
    }
    
    // Option 2: Check error message patterns
    String message = exception.getMessage();
    if (message != null) {
        String lowerMsg = message.toLowerCase();
        return containsAny(lowerMsg, "keyword1", "keyword2", "keyword3");
    }
    
    // Option 3: Check exception class name (for optional dependencies)
    String className = exception.getClass().getName();
    if (className.contains("YourExceptionPattern")) {
        return true;
    }
    
    return false;
}
```

**Example:**
```java
/**
 * Checks if the exception is a file system error.
 */
private static boolean isFileSystemError(Throwable exception) {
    // Check exception type
    if (exception instanceof java.nio.file.FileSystemException ||
        exception instanceof java.io.FileNotFoundException) {
        return true;
    }
    
    // Check error message patterns
    String message = exception.getMessage();
    if (message != null) {
        String lowerMsg = message.toLowerCase();
        return containsAny(lowerMsg, "file not found", "permission denied", 
                          "access denied", "no such file", "cannot access");
    }
    
    return false;
}
```

## Complete Example

Let's add a `FILE_SYSTEM_ERROR` category:

### 1. Add Enum Constant
```java
/**
 * File system errors (e.g., file not found, permission denied).
 */
FILE_SYSTEM_ERROR("File system error", "File system operation failure"),

UNKNOWN("Unknown error", "Unclassified or unknown error type");
```

### 2. Add to categorize() Method
```java
// File system errors
if (isFileSystemError(exception)) {
    return FILE_SYSTEM_ERROR;
}

// Default to application error (must be last)
return APPLICATION_ERROR;
```

### 3. Add Helper Method
```java
/**
 * Checks if the exception is a file system error.
 */
private static boolean isFileSystemError(Throwable exception) {
    if (exception instanceof java.nio.file.FileSystemException ||
        exception instanceof java.io.FileNotFoundException) {
        return true;
    }
    
    String message = exception.getMessage();
    if (message != null) {
        String lowerMsg = message.toLowerCase();
        return containsAny(lowerMsg, "file not found", "permission denied", 
                          "access denied", "no such file");
    }
    
    return false;
}
```

## Important Rules

### ✅ DO:

1. **Add enum constant BEFORE UNKNOWN** - UNKNOWN must always be last
2. **Add check BEFORE APPLICATION_ERROR** - APPLICATION_ERROR is the default fallback
3. **Order matters** - Most specific checks should come first
4. **Use `containsAny()` helper** - For message-based pattern matching
5. **Test your changes** - Verify with actual exceptions

### ❌ DON'T:

1. **Don't add after UNKNOWN** - UNKNOWN must be last
2. **Don't add after APPLICATION_ERROR** - It will never be reached
3. **Don't forget the helper method** - The check needs the helper
4. **Don't use exact string matching** - Use `containsAny()` for flexibility

## Testing Your New Category

After adding a new category, test it:

```java
// Test with actual exception
try {
    throw new FileNotFoundException("File not found: test.txt");
} catch (Exception e) {
    ErrorCategory category = ErrorCategory.categorize(e);
    System.out.println("Category: " + category); // Should print: FILE_SYSTEM_ERROR
}
```

## Common Patterns

### Pattern 1: Exception Type Check
```java
if (exception instanceof YourExceptionType) {
    return YOUR_CATEGORY;
}
```

### Pattern 2: Message Pattern Check
```java
String message = exception.getMessage();
if (message != null) {
    String lowerMsg = message.toLowerCase();
    if (containsAny(lowerMsg, "keyword1", "keyword2")) {
        return YOUR_CATEGORY;
    }
}
```

### Pattern 3: Class Name Check (for optional dependencies)
```java
String className = exception.getClass().getName();
if (className.contains("YourExceptionPattern")) {
    return YOUR_CATEGORY;
}
```

### Pattern 4: Combined Check
```java
if (exception instanceof YourExceptionType ||
    (exception.getMessage() != null && 
     exception.getMessage().toLowerCase().contains("keyword"))) {
    return YOUR_CATEGORY;
}
```

## File Locations

- **Enum Definition:** `src/main/java/com/di/streamnova/aspect/ErrorCategory.java`
- **Usage:** `src/main/java/com/di/streamnova/aspect/TransactionEventAspect.java`

## Verification Checklist

After adding a new category:

- [ ] Enum constant added (before UNKNOWN)
- [ ] Check added in `categorize()` method (before APPLICATION_ERROR)
- [ ] Helper method implemented
- [ ] Code compiles successfully
- [ ] Tested with actual exception
- [ ] Documentation updated (if needed)

## Need Help?

If you're unsure where to add a new category:
1. Look for the comment: `// ========== ADD NEW ERROR CATEGORIES HERE ==========`
2. Follow the examples in the file
3. Check existing patterns (e.g., `isSerializationError()`)

## Summary

**3 Simple Steps:**
1. Add enum constant (before UNKNOWN)
2. Add check in `categorize()` (before APPLICATION_ERROR)
3. Add helper method (use template)

That's it! Your new error category will be automatically used by the AOP system.
