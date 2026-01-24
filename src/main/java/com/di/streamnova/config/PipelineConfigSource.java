package com.di.streamnova.config;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import java.util.List;

@Data
@NoArgsConstructor
public class PipelineConfigSource {
    private String type;
    private String driver;
    private String jdbcUrl;
    private String username;
    @ToString.Exclude
    private String password;
    private String table;
    private int fetchSize;
    private String upperBoundColumn;
    private String lowerBoundColumn;
    private int numPartitions;
    private int maxColumns;
    private List<String> orderBy;
    private int maximumPoolSize;
    private int minimumIdle;
    private long idleTimeout ;      //  ms
    private long connectionTimeout; // ms
    private long maxLifetime ;     // ms
    private Integer shards;        // optional - user-provided shard count (overrides calculation)
    private Integer partitions;    // optional - user-provided partition count (overrides calculation)
    private Integer workers;       // optional - user-provided worker count (overrides calculation)
    private String machineType;    // optional - user-provided machine type (e.g., "n2-standard-4", "n2-highcpu-8")
    private Double  fetchFactor;   // optional (e.g., 1.25)
}
