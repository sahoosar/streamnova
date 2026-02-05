package com.di.streamnova.controller.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Response when table statistics are requested for multiple sources (e.g. all supported types).
 * Success entries return stats; types that are not implemented or fail return error details.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TableStatisticsAggregateResponse {

    /** Success: one entry per source that returned statistics. */
    private List<TableStatisticsResultItem> results;

    /** Errors: one entry per source that failed (not implemented, connection error, etc.). */
    private List<SourceErrorDetail> errors;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TableStatisticsResultItem {
        private String sourceKey;
        private String table;
        private long rowCount;
        private int avgRowSizeBytes;
    }
}
