package com.di.streamnova.controller.dto;

import com.di.streamnova.util.PoolStatsSnapshot;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Response for GET /api/pipeline/supported-sources: shows which supported-source-types are in use
 * (one or both) and for each the connection pool config and live pool stats when available.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SupportedSourcesResponse {

    /** Parsed list from streamnova.statistics.supported-source-types (e.g. ["postgres"] or ["postgres", "oracle"]). */
    private List<String> supportedSourceTypes;

    /** Success: for each type that has config and could be loaded. */
    private List<SourceWithPoolInfo> sources;

    /** Errors: types mentioned in properties but config missing or failed (production-style error details). */
    private List<SourceErrorDetail> errors;

    /** All current pool statistics (HikariCP); may include pools for postgres and oracle. */
    private List<PoolStatsSnapshot> poolStatistics;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class SourceWithPoolInfo {
        private String sourceKey;
        private String type;
        private String jdbcUrlMasked;
        private String username;
        private String table;
        private ConnectionPoolConfig connectionPool;
        /** Live pool stats when a pool exists for this source (same jdbcUrl+user); null if not yet created. */
        private PoolStatsSnapshot livePoolStats;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ConnectionPoolConfig {
        private int maximumPoolSize;
        private int fallbackPoolSize;
        private int minimumIdle;
        private long idleTimeoutMs;
        private long connectionTimeoutMs;
        private long maxLifetimeMs;
    }
}
