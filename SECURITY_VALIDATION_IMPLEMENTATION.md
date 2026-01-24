# Security Input Validation Implementation

## Overview

Comprehensive input validation has been implemented to prevent SQL injection attacks and ensure data integrity across `ShardPlanner` and `PostgresHandler`.

## Implementation Location

**Main Class:** `src/main/java/com/di/streamnova/util/InputValidator.java`

## Security Features

### 1. SQL Identifier Validation

#### Table Name Validation
- **Location:** `PostgresHandler.read()` - Line 80
- **Method:** `InputValidator.validateTableName()`
- **Validates:**
  - Not null or empty
  - Length constraints (1-64 characters)
  - SQL injection pattern detection
  - Valid identifier format (alphanumeric, underscores, dollar signs)
  - Supports schema.table format

#### Schema Name Validation
- **Location:** `PostgresHandler.read()` - Line 92
- **Method:** `InputValidator.validateSchemaName()`
- **Validates:** Same as table name validation

#### Column Name Validation
- **Location:** `DatabaseMetadataDiscoverer.discoverShardExpression()` - Line 505
- **Location:** `DatabaseMetadataDiscoverer.discoverOrderByColumns()` - Line 559
- **Method:** `InputValidator.validateColumnName()`
- **Validates:** Same as table name validation

### 2. SQL Injection Prevention

**Pattern Detection:**
```java
// Detects dangerous SQL patterns:
- SQL keywords: SELECT, INSERT, UPDATE, DELETE, DROP, CREATE, ALTER, etc.
- SQL operators: UNION, OR, AND
- SQL comments: --, /* */
- Statement terminators: ;
- Quotes: ', "
```

**Example:**
```java
// ❌ Rejected: "users; DROP TABLE users--"
// ✅ Accepted: "users"
// ✅ Accepted: "user_data"
// ✅ Accepted: "user$table"
```

### 3. Numeric Input Validation

#### Shard Count Validation
- **Location:** `ShardPlanner.calculateOptimalShardCount()` - Line 199
- **Location:** `PostgresHandler.calculateShardCount()` - Lines 165, 171
- **Method:** `InputValidator.validateShardCount()`
- **Bounds:** 1 to 10,000

#### Fetch Size Validation
- **Location:** `PostgresHandler.read()` - Line 112
- **Method:** `InputValidator.validateFetchSize()`
- **Bounds:** 1 to 100,000

#### Pool Size Validation
- **Location:** `PostgresHandler.read()` - Line 82
- **Location:** `ShardPlanner.calculateOptimalShardCount()` - Line 160
- **Method:** `InputValidator.validatePoolSize()`
- **Bounds:** 1 to 1,000 (null allowed for unlimited)

#### Row Count Validation
- **Location:** `ShardPlanner.calculateOptimalShardCount()` - Line 156
- **Method:** `InputValidator.validateRowCount()`
- **Bounds:** 0 to Long.MAX_VALUE (null allowed)

#### Row Size Validation
- **Location:** `ShardPlanner.calculateOptimalShardCount()` - Line 158
- **Method:** `InputValidator.validateRowSizeBytes()`
- **Bounds:** 1 byte to 10MB (null allowed)

### 4. JDBC URL Validation

- **Location:** `PostgresHandler.read()` - Line 81
- **Method:** `InputValidator.validateJdbcUrl()`
- **Validates:**
  - Not null or empty
  - Must start with "jdbc:"
  - SQL injection pattern detection

## Validation Flow

```
┌─────────────────────────────────────────────────────────┐
│              PostgresHandler.read()                      │
│                                                          │
│  1. validateTableName()          ✅ SQL injection check │
│  2. validateJdbcUrl()             ✅ URL format check    │
│  3. validatePoolSize()            ✅ Bounds check       │
│  4. validateFetchSize()           ✅ Bounds check       │
│  5. validateSchemaName()          ✅ SQL injection check │
│  6. validateColumnName()          ✅ SQL injection check │
│                                                          │
└─────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│        ShardPlanner.calculateOptimalShardCount()        │
│                                                          │
│  1. validateRowCount()          ✅ Bounds check         │
│  2. validateRowSizeBytes()       ✅ Bounds check         │
│  3. validatePoolSize()           ✅ Bounds check         │
│  4. validateShardCount()         ✅ Bounds check         │
│                                                          │
└─────────────────────────────────────────────────────────┘
```

## Security Patterns Detected

The validator detects and rejects:

1. **SQL Keywords:**
   - `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `DROP`, `CREATE`, `ALTER`, `TRUNCATE`, `EXEC`, `EXECUTE`

2. **SQL Operators:**
   - `UNION`, `OR`, `AND`

3. **SQL Comments:**
   - `--` (single-line comment)
   - `/* */` (multi-line comment)

4. **Statement Terminators:**
   - `;` (statement separator)

5. **Quotes:**
   - `'` (single quote)
   - `"` (double quote)

## Example Validation Scenarios

### ✅ Valid Inputs
```java
// Table names
"users"
"user_data"
"schema.users"
"user$table"
"my_table_123"

// Column names
"user_id"
"created_at"
"status"

// Numeric values
shardCount = 10        // ✅ Valid (1-10,000)
fetchSize = 1000      // ✅ Valid (1-100,000)
poolSize = 50         // ✅ Valid (1-1,000)
```

### ❌ Invalid Inputs (Rejected)
```java
// SQL injection attempts
"users; DROP TABLE users--"           // ❌ Contains ; and --
"users' OR '1'='1"                    // ❌ Contains OR and quotes
"users UNION SELECT * FROM passwords" // ❌ Contains UNION and SELECT
"users/*"                             // ❌ Contains comment

// Invalid formats
""                                    // ❌ Empty
"a" * 100                             // ❌ Too long (>64 chars)
"123table"                            // ❌ Starts with digit

// Out of bounds
shardCount = 0                        // ❌ Below minimum
shardCount = 100000                   // ❌ Above maximum
fetchSize = -1                        // ❌ Negative
```

## Error Messages

Validation failures throw `IllegalArgumentException` with descriptive messages:

```java
// Example error messages:
"Table name cannot be null"
"Table name exceeds maximum length of 64 characters: very_long_table_name..."
"Invalid table name format: '123table'. Must start with a letter or underscore..."
"Invalid table name: contains potentially dangerous SQL patterns..."
"Shard count must be at least 1, got: 0"
"Fetch size exceeds maximum of 100000, got: 200000"
```

## Integration Points

### PostgresHandler
- **Line 80:** Table name validation
- **Line 81:** JDBC URL validation
- **Line 82:** Pool size validation
- **Line 84:** Fetch size validation
- **Line 92:** Schema name validation
- **Line 93:** Table name validation (after split)
- **Line 505:** Column name validation (upperBoundColumn)
- **Line 559:** Column name validation (orderBy)
- **Line 112:** Fetch size validation (calculated)
- **Line 165:** Shard count validation (user-provided)
- **Line 171:** Shard count validation (calculated)

### ShardPlanner
- **Line 156:** Row count validation
- **Line 158:** Row size validation
- **Line 160:** Pool size validation
- **Line 199:** Shard count validation (final result)

## Security Benefits

1. **SQL Injection Prevention:**
   - All table/schema/column names validated before use
   - Dangerous patterns detected and rejected
   - Whitelist-based validation (only safe characters allowed)

2. **Data Integrity:**
   - Numeric inputs bounded to prevent overflow/underflow
   - Prevents resource exhaustion (e.g., too many shards)

3. **Defense in Depth:**
   - Validation at multiple layers
   - Early rejection of invalid inputs
   - Clear error messages for debugging

4. **Audit Trail:**
   - Validation failures logged with warnings
   - Helps identify potential attack attempts

## Testing Recommendations

Test cases should cover:
1. ✅ Valid inputs (should pass)
2. ❌ SQL injection patterns (should fail)
3. ❌ Out-of-bounds numeric values (should fail)
4. ❌ Null/empty inputs (should fail)
5. ❌ Invalid formats (should fail)

## Future Enhancements

Potential additions:
- [ ] Rate limiting for validation failures (prevent brute force)
- [ ] More sophisticated SQL injection detection
- [ ] Support for quoted identifiers with validation
- [ ] Custom validation rules per environment
- [ ] Validation metrics (track rejection rates)
