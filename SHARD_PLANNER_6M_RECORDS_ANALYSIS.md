# ShardPlanner Behavior for 6 Million Records

## 📊 Scenario Selection for 6M Records

### **Thresholds:**
- **MEDIUM_THRESHOLD** = 5,000,000 (5M records)
- **LARGE_THRESHOLD** = 10,000,000 (10M records)

### **For 6,000,000 records:**
```
6,000,000 < 5,000,000?  ❌ NO
6,000,000 <= 10,000,000? ✅ YES
```

**Result**: ShardPlanner will use **LARGE scenario** (5M-10M), NOT Medium.

---

## 🎯 Actual Behavior: LARGE Scenario (Correct)

### **Scenario Selection Logic:**
```java
if (estimatedRowCount < 5_000_000L) {
    // MEDIUM (1M-5M)
} else if (estimatedRowCount <= 10_000_000L) {
    // LARGE (5M-10M) ← 6M records will use this
}
```

### **LARGE Scenario Configuration:**
- **Optimal records per shard**: 50,000
- **Maximum records per shard**: 100,000
- **Maximum shards**: 500
- **Strategy**: Cost-effective with good parallelism

### **Calculation for 6M Records (LARGE scenario):**

1. **Records-based calculation:**
   - Optimal: 6,000,000 / 50,000 = **120 shards**
   - Minimum (max records): 6,000,000 / 100,000 = **60 shards**
   - Base shard count: max(120, 60) = **120 shards**

2. **Size-based calculation** (if row size known):
   - Example: 6M × 200 bytes = 1.2 GB = 1,200 MB
   - Size-based: 1,200 MB / 200 MB = **6 shards**
   - Final: max(120, 6) = **120 shards** (records-based wins)

3. **Cost optimization:**
   - If workers limited, may reduce to fit available workers
   - Caps at 500 shards maximum

4. **Final result:**
   - **~120 shards** (if no worker constraints)
   - **~50,000 records per shard**
   - **Cost-optimized** for large dataset

---

## ⚠️ Hypothetical: If MEDIUM Scenario Was Used (Edge Case)

**Note**: This should NOT happen for 6M records, but here's what would occur:

### **MEDIUM Scenario Configuration:**
- **Optimal records per shard**: 25,000
- **Maximum records per shard**: 50,000
- **Maximum shards**: 200
- **Strategy**: Balanced cost/performance

### **If 6M Records Used MEDIUM Logic:**

1. **Records-based calculation:**
   - Optimal: 6,000,000 / 25,000 = **240 shards**
   - Minimum (max records): 6,000,000 / 50,000 = **120 shards**
   - Base: max(240, 120) = **240 shards**

2. **Constraint application:**
   - **Capped at 200 shards** (MAX_SHARDS_MEDIUM)
   - Final: **200 shards** (capped)

3. **Records per shard:**
   - 6,000,000 / 200 = **30,000 records per shard**
   - Within MEDIUM range (25K-50K) ✅

4. **Worker optimization:**
   - If workers limited, may reduce further
   - Cost optimization may apply

### **Comparison:**

| Scenario | Shards | Records/Shard | Max Shards | Cost Impact |
|----------|--------|----------------|------------|-------------|
| **LARGE** (correct) | ~120 | ~50,000 | 500 | Lower cost |
| **MEDIUM** (wrong) | 200 (capped) | 30,000 | 200 | Higher cost |

**Issue**: MEDIUM scenario would create **more shards (200 vs 120)** and **higher cost** because:
- It targets smaller records-per-shard (25K vs 50K)
- It's designed for smaller datasets (1M-5M)
- It's less cost-optimized for large datasets

---

## ✅ Why LARGE Scenario is Correct for 6M Records

### **Benefits of LARGE Scenario:**
1. **Cost Efficiency**: 
   - Fewer shards (120 vs 200) = lower cost
   - Larger records-per-shard (50K vs 30K) = better throughput

2. **Performance**:
   - Optimized for large datasets
   - Better worker utilization
   - Appropriate parallelism for 6M records

3. **Scalability**:
   - Can scale up to 500 shards if needed
   - Better suited for datasets > 5M records

---

## 🔍 Boundary Behavior Analysis

### **At 5M Records (Boundary):**
- **4,999,999 records**: Uses MEDIUM scenario
- **5,000,000 records**: Uses LARGE scenario
- **5,000,001 records**: Uses LARGE scenario

### **At 10M Records (Boundary):**
- **9,999,999 records**: Uses LARGE scenario
- **10,000,000 records**: Uses LARGE scenario
- **10,000,001 records**: Uses VERY LARGE scenario (same logic, extended)

---

## 📝 Example Calculation: 6M Records

### **Input:**
- Records: 6,000,000
- Row size: 200 bytes (optional)
- Machine: n2-standard-4 (4 vCPUs)
- Workers: 8 (or calculated automatically)

### **Step-by-Step:**

1. **Scenario Detection:**
   ```
   6,000,000 >= 5,000,000 ✅
   6,000,000 <= 10,000,000 ✅
   → LARGE scenario selected
   ```

2. **Size Calculation** (if row size known):
   ```
   Total size = 6,000,000 × 200 bytes = 1,200 MB
   Size-based shards = 1,200 MB / 200 MB = 6 shards
   ```

3. **Records-Based Calculation:**
   ```
   Optimal shards = 6,000,000 / 50,000 = 120 shards
   Min shards (max records) = 6,000,000 / 100,000 = 60 shards
   Records-based = max(120, 60) = 120 shards
   ```

4. **Final Selection:**
   ```
   Base shards = max(6, 120) = 120 shards (records-based wins)
   ```

5. **Worker Optimization:**
   ```
   Shards per worker = 4 (standard machine)
   Workers needed = ceil(120 / 4) = 30 workers
   If workers = 8: may reduce to 8 × 4 = 32 shards (cost optimization)
   ```

6. **Constraint Application:**
   ```
   Max shards (LARGE) = 500 ✅ (120 < 500)
   Pool capacity = 80% of pool size
   Final = min(120, pool_capacity, 500) = 120 shards
   ```

7. **Final Result:**
   ```
   Shards: 120
   Records per shard: 50,000
   Workers needed: 30 (or 8 if cost-optimized)
   ```

---

## 🎯 Summary

### **For 6 Million Records:**

✅ **Correct Behavior**: Uses **LARGE scenario** (5M-10M)
- Optimal: ~120 shards
- Records per shard: ~50,000
- Cost-optimized for large datasets

❌ **If MEDIUM was used** (shouldn't happen):
- Would create 200 shards (capped)
- Records per shard: 30,000
- Higher cost, less efficient

### **Key Points:**
1. **6M records correctly uses LARGE scenario** (not MEDIUM)
2. **Boundary is at 5M**: < 5M = MEDIUM, >= 5M = LARGE
3. **LARGE scenario is more cost-efficient** for 6M records
4. **Code correctly handles the boundary** with proper threshold checks

---

## 🔧 Code Verification

The scenario selection logic is:
```java
} else if (estimatedRowCount < DatasetScenarioConfig.MEDIUM_THRESHOLD) {
    // MEDIUM (1M-5M) - only for < 5M
} else if (estimatedRowCount <= DatasetScenarioConfig.LARGE_THRESHOLD) {
    // LARGE (5M-10M) - for 6M records ✅
}
```

**Conclusion**: The code is **correct** - 6M records will use LARGE scenario, not MEDIUM.
