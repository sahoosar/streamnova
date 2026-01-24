package com.di.streamnova.util.shardplanner;

import lombok.extern.slf4j.Slf4j;

/**
 * Calculates data size information from row count and average row size.
 */
@Slf4j
public final class DataSizeCalculator {
    private DataSizeCalculator() {}
    
    public static DataSizeInfo calculateDataSize(Long estimatedRowCount, Integer averageRowSizeBytes) {
        if (estimatedRowCount != null && estimatedRowCount > 0 
                && averageRowSizeBytes != null && averageRowSizeBytes > 0) {
            long totalSizeBytes = estimatedRowCount * averageRowSizeBytes;
            double totalSizeMb = totalSizeBytes / (1024.0 * 1024.0);
            double totalSizeGb = totalSizeBytes / (1024.0 * 1024.0 * 1024.0);
            
            log.info("Record size calculation: {} rows × {} bytes/row = {} bytes total", 
                    estimatedRowCount, averageRowSizeBytes, totalSizeBytes);
            log.info("Total output size: {} MB ({} GB) - calculated from {} rows × {} bytes/row", 
                    String.format("%.2f", totalSizeMb), String.format("%.2f", totalSizeGb), 
                    estimatedRowCount, averageRowSizeBytes);
            
            return new DataSizeInfo(totalSizeMb, totalSizeBytes);
        } else {
            if (estimatedRowCount == null || estimatedRowCount <= 0) {
                log.warn("Cannot calculate record size: estimatedRowCount is missing or invalid: {}", estimatedRowCount);
            }
            if (averageRowSizeBytes == null || averageRowSizeBytes <= 0) {
                log.warn("Cannot calculate record size: averageRowSizeBytes is missing or invalid: {}", averageRowSizeBytes);
            }
            return new DataSizeInfo(null, null);
        }
    }
}
