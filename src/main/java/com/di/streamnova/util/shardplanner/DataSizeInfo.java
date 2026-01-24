package com.di.streamnova.util.shardplanner;

/**
 * Represents calculated data size information.
 */
public final class DataSizeInfo {
    public final Double totalSizeMb;
    public final Long totalSizeBytes;
    public final boolean hasSizeInformation;
    
    public DataSizeInfo(Double totalSizeMb, Long totalSizeBytes) {
        this.totalSizeMb = totalSizeMb;
        this.totalSizeBytes = totalSizeBytes;
        this.hasSizeInformation = (totalSizeMb != null && totalSizeMb > 0);
    }
}
