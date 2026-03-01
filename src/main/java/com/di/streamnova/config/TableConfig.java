package com.di.streamnova.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Per-table config: which connection to use and table name/options.
 * Defined under pipeline.config.tables.&lt;key&gt; in YAML; the key is used as API source (?source=key).
 * Only connection and table are required; other fields override the connection's values when set.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TableConfig {
    /** Connection name (e.g. postgres, oracle). Must be a key in pipeline.config.connections. Required. */
    private String connection;
    /** Full table name (e.g. market_summary or schema.some_table). Required. */
    private String table;

    /** Schema name (for AI tool / profiler). */
    private String schema;
    /** Table name only (for AI tool / profiler). */
    private String tableName;
    /** Estimated row count (for AI recommendation tool). */
    private Long estimatedRows;
    /** Estimated table size in MB (for AI recommendation tool). */
    private Double estimatedSizeMb;

    /** Optional. Max columns to read (0 = no limit). Omit to use connection's value. */
    private Integer maxColumns;
    /** Optional. Column for hash-based sharding or partition filter. Omit if not used. */
    private String upperBoundColumn;
    /** Optional. Partition value to filter (e.g. date, id). Use with upperBoundColumn. */
    private String partitionValue;
    /** Optional. Start of partition range (e.g. date). Use with partitionEndValue and upperBoundColumn for date-range load. */
    private String partitionStartValue;
    /** Optional. End of partition range (e.g. date). Use with partitionStartValue and upperBoundColumn. */
    private String partitionEndValue;
    /** Optional. Fallback shard/ordering column when no PK or partition. */
    private String shardColumn;
    /** Optional. ORDER BY columns. Omit to use connection's value. */
    private List<String> orderBy;
}
