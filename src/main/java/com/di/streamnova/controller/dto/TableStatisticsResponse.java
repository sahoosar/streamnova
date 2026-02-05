package com.di.streamnova.controller.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * REST response for table statistics (row count and average row size).
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TableStatisticsResponse {
    private String table;
    private long rowCount;
    private int avgRowSizeBytes;
}
