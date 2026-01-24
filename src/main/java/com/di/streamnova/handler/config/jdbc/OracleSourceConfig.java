package com.di.streamnova.handler.config.jdbc;

import java.util.HashMap;
import java.util.Map;
import com.di.streamnova.handler.config.SourceConfig;
import lombok.Data;
import lombok.Builder;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OracleSourceConfig implements SourceConfig {
    @Builder.Default
    private String type = "oracle";
    private String jdbcUrl;
    private String username;
    private String password;
    private String table;
    @Builder.Default
    private int fetchSize = 1000; // Default fetch size
    private String upperBoundColumn; // Column name for partitioning
    private String lowerBoundColumn; // Column name for partitioning
    private Long upperBound; // Numeric upper bound value for partitioning (e.g., max ID or max date timestamp)
    private Long lowerBound; // Numeric lower bound value for partitioning (e.g., min ID or min date timestamp)
    @Builder.Default
    private int numPartitions = 10; // Default number of partitions
    private String query; // Optional SQL query instead of table

    @Override
    public String getType() {
        return type;
    }

    @Override
    public Map<String, Object> getProperties() {
        
        Map<String, Object> properties = new HashMap<>();

        properties.put("type", type);
        properties.put("driver", "oracle.jdbc.OracleDriver");
        properties.put("url", jdbcUrl);
        properties.put("username", username);
        properties.put("password", password);

        // Add table or query
        if (query != null && !query.trim().isEmpty()) {
            properties.put("query", query);
        } else {
            properties.put("table", table);
        }

        // Add partitioning properties if partition column and bounds are specified
        // For JDBC partitioning, we need:
        // - partitionColumn: the column name to partition on
        // - lowerBound: numeric minimum value (not column name)
        // - upperBound: numeric maximum value (not column name)
        // - numPartitions: number of partitions
        if (upperBoundColumn != null && !upperBoundColumn.isBlank()) {
            properties.put("partitionColumn", upperBoundColumn);
            
            // Use actual numeric bounds if provided, otherwise use column names as fallback (legacy behavior)
            if (lowerBound != null) {
                properties.put("lowerBound", lowerBound);
            } else if (lowerBoundColumn != null && !lowerBoundColumn.isBlank()) {
                // Legacy: if only column name provided, log warning and don't set (will fail at runtime)
                log.warn("lowerBoundColumn '{}' provided but lowerBound value is missing. " +
                        "JDBC partitioning requires numeric lowerBound value, not column name.", lowerBoundColumn);
            }
            
            if (upperBound != null) {
                properties.put("upperBound", upperBound);
            } else {
                // Legacy: if only column name provided, log warning
                log.warn("upperBoundColumn '{}' provided but upperBound value is missing. " +
                        "JDBC partitioning requires numeric upperBound value, not column name.", upperBoundColumn);
            }
            
            if (numPartitions > 0) {
                properties.put("numPartitions", numPartitions);
            }
        }

        properties.put("fetchSize", fetchSize);

        return properties;
    }
}
