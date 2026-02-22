package com.di.streamnova.handler.jdbc.postgres;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configurable thresholds and limits for adaptive JDBC fetch size in PostgresHandler.
 * When table row count exceeds a threshold, fetch size is increased to reduce round trips.
 *
 * <p>Bound from application.yml under {@code streamnova.adaptive-fetch}.
 */
@ConfigurationProperties(prefix = "streamnova.adaptive-fetch")
public class AdaptiveFetchProperties {

    /** Row count above which "large dataset" rules apply. Default 10M. */
    private long largeDatasetThresholdRows = 10_000_000L;
    /** Max fetch size for large datasets. Default 50000. */
    private int largeDatasetMaxFetchSize = 50_000;
    /** Multiplier for large datasets: fetchSize = min(max, base * this). Default 10. */
    private int largeDatasetMultiplier = 10;

    /** Row count above which "medium dataset" rules apply (below large). Default 1M. */
    private long mediumDatasetThresholdRows = 1_000_000L;
    /** Max fetch size for medium datasets. Default 20000. */
    private int mediumDatasetMaxFetchSize = 20_000;
    /** Multiplier for medium datasets. Default 4. */
    private int mediumDatasetMultiplier = 4;

    public long getLargeDatasetThresholdRows() {
        return largeDatasetThresholdRows;
    }

    public void setLargeDatasetThresholdRows(long largeDatasetThresholdRows) {
        this.largeDatasetThresholdRows = largeDatasetThresholdRows;
    }

    public int getLargeDatasetMaxFetchSize() {
        return largeDatasetMaxFetchSize;
    }

    public void setLargeDatasetMaxFetchSize(int largeDatasetMaxFetchSize) {
        this.largeDatasetMaxFetchSize = largeDatasetMaxFetchSize;
    }

    public int getLargeDatasetMultiplier() {
        return largeDatasetMultiplier;
    }

    public void setLargeDatasetMultiplier(int largeDatasetMultiplier) {
        this.largeDatasetMultiplier = largeDatasetMultiplier;
    }

    public long getMediumDatasetThresholdRows() {
        return mediumDatasetThresholdRows;
    }

    public void setMediumDatasetThresholdRows(long mediumDatasetThresholdRows) {
        this.mediumDatasetThresholdRows = mediumDatasetThresholdRows;
    }

    public int getMediumDatasetMaxFetchSize() {
        return mediumDatasetMaxFetchSize;
    }

    public void setMediumDatasetMaxFetchSize(int mediumDatasetMaxFetchSize) {
        this.mediumDatasetMaxFetchSize = mediumDatasetMaxFetchSize;
    }

    public int getMediumDatasetMultiplier() {
        return mediumDatasetMultiplier;
    }

    public void setMediumDatasetMultiplier(int mediumDatasetMultiplier) {
        this.mediumDatasetMultiplier = mediumDatasetMultiplier;
    }
}
