package com.di.streamnova.load.plan;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * Queries the source table for its partition-column range and slices it into
 * equal-width {@link ShardRange} objects.
 *
 * <p>All SQL is built with quoted identifiers to handle mixed-case names.
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class ShardBoundaryCalculator {

    private final JdbcTemplate jdbcTemplate;

    /* ------------------------------------------------------------------ */
    /* Public API                                                           */
    /* ------------------------------------------------------------------ */

    /**
     * Returns {@code [minKey, maxKey]} for the given partition column.
     *
     * @throws IllegalStateException if the table is empty or the column has NULLs only
     */
    public long[] fetchMinMax(String schema, String table, String partitionCol) {
        String sql = "SELECT MIN(" + qi(partitionCol) + "), MAX(" + qi(partitionCol) + ")"
                   + " FROM " + qi(schema) + "." + qi(table);

        long[] result = new long[2];
        Boolean found = jdbcTemplate.query(sql, rs -> {
            if (!rs.next()) return null;
            result[0] = rs.getLong(1);
            result[1] = rs.getLong(2);
            return !rs.wasNull();
        });

        if (Boolean.FALSE.equals(found)) {
            throw new IllegalStateException(
                "Table " + schema + "." + table + " has no rows or '" + partitionCol + "' is NULL");
        }
        log.info("[SHARD-BOUNDS] {}.{}.{}: min={} max={}", schema, table, partitionCol,
                 result[0], result[1]);
        return result;
    }

    /**
     * Divides [{@code minKey}, {@code maxKey}] into {@code shardCount}
     * equal-width, non-overlapping ranges.
     */
    public List<ShardRange> computeRanges(long minKey, long maxKey, int shardCount) {
        if (shardCount <= 0) throw new IllegalArgumentException("shardCount must be > 0");

        long totalRange  = maxKey - minKey + 1;
        long shardWidth  = (totalRange + shardCount - 1) / shardCount; // ceiling division

        List<ShardRange> ranges = new ArrayList<>(shardCount);
        for (int i = 0; i < shardCount; i++) {
            long lo = minKey + (long) i * shardWidth;
            long hi = (i == shardCount - 1) ? maxKey : lo + shardWidth - 1;
            ranges.add(ShardRange.builder()
                    .shardIndex(i)
                    .minKey(lo)
                    .maxKey(hi)
                    .estimatedRows(hi - lo + 1)  // uniform approximation
                    .pilot(false)                 // caller sets pilot flag when needed
                    .build());
        }
        return ranges;
    }

    /**
     * Re-slices the key space for shards {@code startIdx..end} using a new
     * {@code newShardCount} for the remaining rows.  Shards {@code 0..startIdx-1}
     * are preserved verbatim (they are already done or in-flight).
     *
     * <p>Called by iteration 3 when the pilot reveals the shard size needs
     * adjustment.
     */
    public List<ShardRange> recomputeFromIndex(
            List<ShardRange> existingShards,
            int startIdx,
            long globalMaxKey,
            int newShardCount) {

        if (startIdx >= existingShards.size()) {
            return new ArrayList<>(existingShards);
        }

        long startKey = existingShards.get(startIdx).getMinKey();
        long range    = globalMaxKey - startKey + 1;
        long width    = (range + newShardCount - 1) / newShardCount;

        // Keep already-planned / completed shards before startIdx
        List<ShardRange> result = new ArrayList<>(existingShards.subList(0, startIdx));

        for (int i = 0; i < newShardCount; i++) {
            long lo = startKey + (long) i * width;
            long hi = (i == newShardCount - 1) ? globalMaxKey : lo + width - 1;
            result.add(ShardRange.builder()
                    .shardIndex(startIdx + i)
                    .minKey(lo)
                    .maxKey(hi)
                    .estimatedRows(hi - lo + 1)
                    .pilot(false)
                    .build());
        }
        return result;
    }

    /* ------------------------------------------------------------------ */

    /** Wraps an identifier in double quotes for safe SQL embedding. */
    private static String qi(String name) {
        return "\"" + name.replace("\"", "\"\"") + "\"";
    }
}
