# Multi-Application Oracle Support Analysis

## Current Status: ❌ **NOT READY FOR 10 APPLICATIONS**

## Critical Issues Identified

### 1. ❌ **OracleHandler Not Implemented**

**Location:** `src/main/java/com/di/streamnova/handler/impl/OracleHandler.java`

**Problem:**
```java
@Override
public PCollection<Row> read(Pipeline pipeline, PipelineConfigSource config) {
    return null;  // ❌ NOT IMPLEMENTED
}
```

**Impact:** Oracle support is completely missing. Applications cannot load data from Oracle.

---

### 2. ❌ **HikariDataSourceSingleton - Single DataSource Limitation**

**Location:** `src/main/java/com/di/streamnova/util/HikariDataSourceSingleton.java`

**Critical Problem:**
```java
private transient volatile DataSource dataSource;  // ❌ Only ONE DataSource stored

public DataSource getOrInit(DbConfigSnapshot snapshot) {
    DataSource local = dataSource;
    if (local == null) {  // ❌ Only checks if null, doesn't check if snapshot matches
        // Creates DataSource based on snapshot
        // But if called again with DIFFERENT snapshot, returns SAME DataSource!
    }
    return local;
}
```

**What Happens with 10 Applications:**

```
App 1: Calls getOrInit(Oracle DB1) → Creates DataSource for DB1 ✅
App 2: Calls getOrInit(Oracle DB2) → Returns DataSource for DB1 ❌ WRONG!
App 3: Calls getOrInit(Oracle DB3) → Returns DataSource for DB1 ❌ WRONG!
...
App 10: Calls getOrInit(Oracle DB10) → Returns DataSource for DB1 ❌ WRONG!
```

**Result:** All 10 applications will connect to the FIRST Oracle database, not their own!

---

### 3. ❌ **PostgresHandler Uses PostgreSQL-Specific SQL**

**Location:** `src/main/java/com/di/streamnova/handler/impl/PostgresHandler.java`

**Problem:** The SQL queries use PostgreSQL-specific syntax:
- `md5()` function
- `::text` and `::bit` type casting
- `pg_class`, `pg_stats` system tables
- PostgreSQL-specific column discovery queries

**Example:**
```sql
-- This won't work in Oracle:
WHERE ((((('x'||substr(md5(%s),1,8))::bit(32))::bigint & 2147483647) %% %d)) = ?
```

**Oracle equivalent would need:**
- `DBMS_CRYPTO.HASH()` instead of `md5()`
- `TO_NUMBER()` instead of `::bigint`
- `ALL_TABLES`, `ALL_TAB_COLUMNS` instead of `pg_class`, `pg_stats`

---

### 4. ⚠️ **No Connection Pooling Per Application**

**Problem:** Even if fixed, each application would need its own connection pool configuration.

**Current:** Single shared pool (which is broken)

**Needed:** Per-application or per-database connection pools

---

## Required Fixes

### Fix 1: Implement OracleHandler

Create a complete OracleHandler similar to PostgresHandler but with Oracle-specific:
- SQL syntax
- System table queries
- Type conversions
- Schema detection

### Fix 2: Fix HikariDataSourceSingleton

**Current (BROKEN):**
```java
private transient volatile DataSource dataSource;  // Single instance

public DataSource getOrInit(DbConfigSnapshot snapshot) {
    if (dataSource == null) {
        // Create DataSource
    }
    return dataSource;  // Always returns same one
}
```

**Required (FIXED):**
```java
private final ConcurrentHashMap<String, DataSource> dataSourceCache = new ConcurrentHashMap<>();

public DataSource getOrInit(DbConfigSnapshot snapshot) {
    String key = generateKey(snapshot);  // Key by JDBC URL + username
    return dataSourceCache.computeIfAbsent(key, k -> createDataSource(snapshot));
}

private String generateKey(DbConfigSnapshot snapshot) {
    return snapshot.jdbcUrl() + "|" + snapshot.username();
}
```

### Fix 3: Database-Agnostic SQL Generation

Extract database-specific SQL into strategy pattern:
- `PostgresSqlBuilder`
- `OracleSqlBuilder`
- `DatabaseSqlBuilder` interface

---

## Recommended Solution Architecture

### Option 1: Per-Application Isolation (Recommended)

Each application runs in its own JVM/container:
- ✅ Each has its own `HikariDataSourceSingleton` instance
- ✅ No shared state
- ✅ Independent connection pools
- ✅ No code changes needed (if OracleHandler is implemented)

**Deployment:**
```
App 1 (JVM) → OracleHandler → HikariDataSourceSingleton → Oracle DB1
App 2 (JVM) → OracleHandler → HikariDataSourceSingleton → Oracle DB2
...
App 10 (JVM) → OracleHandler → HikariDataSourceSingleton → Oracle DB10
```

### Option 2: Shared Service with Connection Pooling

Single service handles all applications:
- ⚠️ Requires fixing `HikariDataSourceSingleton` to support multiple connections
- ⚠️ Requires implementing `OracleHandler`
- ⚠️ Requires database-agnostic SQL generation
- ✅ Centralized management
- ✅ Shared resources

---

## Implementation Plan

### Phase 1: Fix HikariDataSourceSingleton (CRITICAL)

**Priority:** 🔴 **CRITICAL** - Blocks all multi-application scenarios

**Changes:**
1. Replace single `DataSource` with `ConcurrentHashMap<String, DataSource>`
2. Key by JDBC URL + username (or full snapshot hash)
3. Thread-safe initialization
4. Connection pool cleanup on shutdown

### Phase 2: Implement OracleHandler

**Priority:** 🔴 **CRITICAL** - Required for Oracle support

**Changes:**
1. Copy PostgresHandler structure
2. Replace PostgreSQL-specific SQL with Oracle equivalents
3. Use Oracle system tables (`ALL_TABLES`, `ALL_TAB_COLUMNS`, etc.)
4. Use Oracle functions (`DBMS_CRYPTO.HASH`, `TO_NUMBER`, etc.)
5. Handle Oracle-specific type conversions

### Phase 3: Database-Agnostic SQL (Optional but Recommended)

**Priority:** 🟡 **MEDIUM** - Improves maintainability

**Changes:**
1. Create `DatabaseSqlBuilder` interface
2. Implement `PostgresSqlBuilder` and `OracleSqlBuilder`
3. Factory pattern for selection
4. Extract database-specific logic

---

## Current Code Assessment

### ✅ What Works for Multi-Application

1. **ShardPlanner** - Stateless, thread-safe ✅
2. **InputValidator** - Stateless, thread-safe ✅
3. **MetricsCollector** - Spring component, per-application instance ✅
4. **PostgresHandler** - Stateless, but PostgreSQL-specific ⚠️

### ❌ What Doesn't Work

1. **HikariDataSourceSingleton** - Single DataSource, not keyed ❌
2. **OracleHandler** - Not implemented ❌
3. **PostgresHandler SQL** - PostgreSQL-specific, won't work for Oracle ❌

---

## Immediate Action Required

### For 10 Applications Using Oracle:

**Option A: Quick Fix (Per-Application Isolation)**
1. ✅ Each application runs in separate JVM/container
2. ❌ Implement OracleHandler (still required)
3. ✅ Fix HikariDataSourceSingleton (still recommended for safety)

**Option B: Proper Fix (Shared Service)**
1. ❌ Fix HikariDataSourceSingleton (REQUIRED)
2. ❌ Implement OracleHandler (REQUIRED)
3. ⚠️ Make SQL generation database-agnostic (RECOMMENDED)

---

## Code Changes Needed

### Minimum Changes (Option A - Per-App Isolation)

1. **Implement OracleHandler** (~500-800 lines)
   - Similar to PostgresHandler
   - Oracle-specific SQL and queries

### Full Changes (Option B - Shared Service)

1. **Fix HikariDataSourceSingleton** (~50 lines)
   - Add connection caching by key
   - Thread-safe initialization

2. **Implement OracleHandler** (~500-800 lines)
   - Complete Oracle implementation

3. **Database-Agnostic SQL** (~200-300 lines)
   - Strategy pattern for SQL generation

---

## Testing Requirements

For multi-application support:
1. ✅ Test with 10 concurrent applications
2. ✅ Test with different Oracle databases
3. ✅ Test connection pool isolation
4. ✅ Test metrics collection per application
5. ✅ Test error handling and recovery

---

## Conclusion

**Current State:** ❌ **NOT READY**

**Blockers:**
1. OracleHandler not implemented
2. HikariDataSourceSingleton doesn't support multiple connections
3. PostgreSQL-specific SQL won't work for Oracle

**Estimated Effort:**
- **Minimum (Per-App Isolation):** 1-2 weeks (OracleHandler only)
- **Full Fix (Shared Service):** 2-3 weeks (All fixes)

**Recommendation:** 
- **Short-term:** Use per-application isolation + implement OracleHandler
- **Long-term:** Fix HikariDataSourceSingleton + database-agnostic SQL
