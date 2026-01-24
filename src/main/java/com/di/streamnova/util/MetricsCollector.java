package com.di.streamnova.util;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

/**
 * Metrics collector for ShardPlanner and PostgresHandler operations.
 * Provides metrics for monitoring shard planning, execution time, and error rates.
 */
@Slf4j
@Component
public class MetricsCollector {

    private final MeterRegistry meterRegistry;

    // Shard Planning Metrics
    private final DistributionSummary shardCountDistribution;
    private final Counter shardPlanningCounter;
    private final Counter shardPlanningErrorCounter;
    private final Timer shardPlanningTimer;

    // Postgres Handler Metrics
    private final Timer postgresReadTimer;
    private final Counter postgresReadCounter;
    private final Counter postgresReadErrorCounter;
    private final Timer schemaDetectionTimer;
    private final Timer statisticsEstimationTimer;
    private final Counter connectionFailureCounter;

    // Execution Environment Metrics
    private final DistributionSummary virtualCpusDistribution;
    private final DistributionSummary workerCountDistribution;
    private final DistributionSummary estimatedRowCountDistribution;

    public MetricsCollector(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;

        // Shard Planning Metrics
        this.shardCountDistribution = DistributionSummary.builder("shardplanner.shard.count")
                .description("Distribution of calculated shard counts")
                .register(meterRegistry);

        this.shardPlanningCounter = Counter.builder("shardplanner.planning.total")
                .description("Total number of shard planning operations")
                .tag("status", "success")
                .register(meterRegistry);

        this.shardPlanningErrorCounter = Counter.builder("shardplanner.planning.total")
                .description("Total number of shard planning errors")
                .tag("status", "error")
                .register(meterRegistry);

        this.shardPlanningTimer = Timer.builder("shardplanner.planning.duration")
                .description("Time taken to calculate optimal shard count")
                .register(meterRegistry);

        // Postgres Handler Metrics
        this.postgresReadTimer = Timer.builder("postgres.handler.read.duration")
                .description("Time taken to read data from PostgreSQL")
                .register(meterRegistry);

        this.postgresReadCounter = Counter.builder("postgres.handler.read.total")
                .description("Total number of PostgreSQL read operations")
                .tag("status", "success")
                .register(meterRegistry);

        this.postgresReadErrorCounter = Counter.builder("postgres.handler.read.total")
                .description("Total number of PostgreSQL read errors")
                .tag("status", "error")
                .register(meterRegistry);

        this.schemaDetectionTimer = Timer.builder("postgres.handler.schema.detection.duration")
                .description("Time taken to detect table schema")
                .register(meterRegistry);

        this.statisticsEstimationTimer = Timer.builder("postgres.handler.statistics.estimation.duration")
                .description("Time taken to estimate table statistics")
                .register(meterRegistry);

        this.connectionFailureCounter = Counter.builder("postgres.handler.connection.failures")
                .description("Number of database connection failures")
                .register(meterRegistry);

        // Execution Environment Metrics
        this.virtualCpusDistribution = DistributionSummary.builder("execution.environment.vcpus")
                .description("Distribution of virtual CPUs detected")
                .register(meterRegistry);

        this.workerCountDistribution = DistributionSummary.builder("execution.environment.workers")
                .description("Distribution of worker counts")
                .register(meterRegistry);

        this.estimatedRowCountDistribution = DistributionSummary.builder("postgres.handler.estimated.row.count")
                .description("Distribution of estimated row counts")
                .baseUnit("rows")
                .register(meterRegistry);
    }

    // ============================================================================
    // Shard Planning Metrics
    // ============================================================================

    /**
     * Records a successful shard planning operation.
     *
     * @param shardCount The calculated shard count
     * @param durationMs The time taken in milliseconds
     */
    public void recordShardPlanning(int shardCount, long durationMs) {
        shardCountDistribution.record(shardCount);
        shardPlanningCounter.increment();
        shardPlanningTimer.record(durationMs, TimeUnit.MILLISECONDS);
        log.debug("Recorded shard planning: shardCount={}, durationMs={}", shardCount, durationMs);
    }

    /**
     * Records a shard planning error.
     */
    public void recordShardPlanningError() {
        shardPlanningErrorCounter.increment();
        log.debug("Recorded shard planning error");
    }

    /**
     * Records shard planning with environment context.
     *
     * @param shardCount The calculated shard count
     * @param durationMs The time taken in milliseconds
     * @param virtualCpus Number of virtual CPUs
     * @param workerCount Number of workers (null for local execution)
     * @param estimatedRowCount Estimated row count
     */
    public void recordShardPlanningWithContext(int shardCount, long durationMs, 
                                                int virtualCpus, Integer workerCount, Long estimatedRowCount) {
        recordShardPlanning(shardCount, durationMs);
        
        virtualCpusDistribution.record(virtualCpus);
        if (workerCount != null) {
            workerCountDistribution.record(workerCount);
        }
        if (estimatedRowCount != null && estimatedRowCount > 0) {
            estimatedRowCountDistribution.record(estimatedRowCount);
        }
    }

    // ============================================================================
    // Postgres Handler Metrics
    // ============================================================================

    /**
     * Records a successful PostgreSQL read operation.
     *
     * @param durationMs The time taken in milliseconds
     */
    public void recordPostgresRead(long durationMs) {
        postgresReadCounter.increment();
        postgresReadTimer.record(durationMs, TimeUnit.MILLISECONDS);
        log.debug("Recorded PostgreSQL read: durationMs={}", durationMs);
    }

    /**
     * Records a PostgreSQL read error.
     */
    public void recordPostgresReadError() {
        postgresReadErrorCounter.increment();
        log.debug("Recorded PostgreSQL read error");
    }

    /**
     * Records schema detection time.
     *
     * @param durationMs The time taken in milliseconds
     */
    public void recordSchemaDetection(long durationMs) {
        schemaDetectionTimer.record(durationMs, TimeUnit.MILLISECONDS);
        log.debug("Recorded schema detection: durationMs={}", durationMs);
    }

    /**
     * Records statistics estimation time.
     *
     * @param durationMs The time taken in milliseconds
     */
    public void recordStatisticsEstimation(long durationMs) {
        statisticsEstimationTimer.record(durationMs, TimeUnit.MILLISECONDS);
        log.debug("Recorded statistics estimation: durationMs={}", durationMs);
    }

    /**
     * Records a database connection failure.
     */
    public void recordConnectionFailure() {
        connectionFailureCounter.increment();
        log.debug("Recorded connection failure");
    }

    // ============================================================================
    // Utility Methods
    // ============================================================================

    /**
     * Creates a timer sample for manual timing.
     *
     * @return A timer sample that can be stopped to record duration
     */
    public Timer.Sample startTimer() {
        return Timer.start(meterRegistry);
    }

    /**
     * Records a custom metric value.
     *
     * @param name Metric name
     * @param value Value to record
     * @param tags Optional tags
     */
    public void recordCustomMetric(String name, double value, String... tags) {
        DistributionSummary.builder(name)
                .register(meterRegistry)
                .record(value);
    }
}
