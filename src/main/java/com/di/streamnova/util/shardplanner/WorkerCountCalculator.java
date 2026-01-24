package com.di.streamnova.util.shardplanner;

import lombok.extern.slf4j.Slf4j;

/**
 * Calculates optimal worker count based on various factors.
 */
@Slf4j
public final class WorkerCountCalculator {
    private WorkerCountCalculator() {}
    
    /**
     * Configuration for worker count calculation.
     */
    private static final class WorkerConfig {
        static final int MIN_WORKERS = 1;
        static final int MAX_WORKERS_SMALL_DATA = 4;      // For < 1M rows
        static final int MAX_WORKERS_MEDIUM_DATA = 16;    // For 1M-10M rows
        static final int MAX_WORKERS_LARGE_DATA = 64;     // For > 10M rows
        static final double WORKERS_PER_GB = 0.5;         // Workers per GB of data
        static final int MIN_WORKERS_PER_100K_ROWS = 1;   // Minimum workers per 100K rows
    }
    
    /**
     * Calculates optimal shards per worker based on machine type.
     */
    public static int calculateOptimalShardsPerWorker(ExecutionEnvironment environment, MachineProfile profile) {
        if (environment.machineType == null || environment.machineType.isBlank()) {
            return CostOptimizationConfig.OPTIMAL_SHARDS_PER_WORKER;
        }
        
        String machineTypeLower = environment.machineType.toLowerCase();
        
        // High-CPU machines: can handle more shards per worker
        if (machineTypeLower.contains("highcpu")) {
            return Math.max(CostOptimizationConfig.OPTIMAL_SHARDS_PER_WORKER, 
                    environment.virtualCpus * 2);
        }
        
        // High-memory machines: balanced shards per worker
        if (machineTypeLower.contains("highmem") || machineTypeLower.contains("memory")) {
            return Math.max(4, environment.virtualCpus);
        }
        
        // Standard machines: based on vCPUs
        return Math.max(CostOptimizationConfig.OPTIMAL_SHARDS_PER_WORKER, environment.virtualCpus);
    }
    
    /**
     * Calculates minimum workers needed based on data size.
     */
    public static int calculateMinimumWorkersForData(Long estimatedRowCount, Integer averageRowSizeBytes,
                                                   ExecutionEnvironment environment, MachineProfile profile) {
        int minWorkers = WorkerConfig.MIN_WORKERS;
        
        // If we have row count, calculate based on data volume
        if (estimatedRowCount != null && estimatedRowCount > 0) {
            // Calculate data size in GB
            long totalBytes = estimatedRowCount * (averageRowSizeBytes != null && averageRowSizeBytes > 0 
                    ? averageRowSizeBytes : 200);
            double totalGb = totalBytes / (1024.0 * 1024.0 * 1024.0);
            
            // Minimum workers based on data size
            int workersFromSize = (int) Math.ceil(totalGb * WorkerConfig.WORKERS_PER_GB);
            
            // Minimum workers based on row count (1 worker per 100K rows minimum)
            int workersFromRows = (int) Math.ceil((double) estimatedRowCount / 100_000.0) 
                    * WorkerConfig.MIN_WORKERS_PER_100K_ROWS;
            
            minWorkers = Math.max(workersFromSize, workersFromRows);
            
            log.debug("Minimum workers calculation: {} GB data → {} workers, {} rows → {} workers, final min={}",
                    String.format("%.2f", totalGb), workersFromSize, estimatedRowCount, workersFromRows, minWorkers);
        }
        
        return Math.max(WorkerConfig.MIN_WORKERS, minWorkers);
    }
    
    /**
     * Calculates maximum workers for cost efficiency.
     */
    public static int calculateMaximumWorkersForCost(int shardCount, ExecutionEnvironment environment, 
                                                   MachineProfile profile) {
        // For small datasets, limit workers to avoid over-provisioning
        int maxWorkers;
        
        if (shardCount < 10) {
            maxWorkers = WorkerConfig.MAX_WORKERS_SMALL_DATA;
        } else if (shardCount < 100) {
            maxWorkers = WorkerConfig.MAX_WORKERS_MEDIUM_DATA;
        } else {
            maxWorkers = WorkerConfig.MAX_WORKERS_LARGE_DATA;
        }
        
        // Machine type considerations
        if (environment.machineType != null) {
            String machineTypeLower = environment.machineType.toLowerCase();
            
            // High-CPU machines are more cost-effective, allow more workers
            if (machineTypeLower.contains("highcpu")) {
                maxWorkers = (int) (maxWorkers * 1.5);
            }
            
            // High-memory machines are more expensive, limit workers
            if (machineTypeLower.contains("highmem") || machineTypeLower.contains("memory")) {
                maxWorkers = (int) (maxWorkers * 0.75);
            }
        }
        
        return maxWorkers;
    }
    
    /**
     * Rounds worker count to optimal value (power of 2 for GCP efficiency).
     */
    public static int roundToOptimalWorkerCount(int workers) {
        if (workers <= 1) {
            return 1;
        }
        
        // Round to nearest power of 2 (1, 2, 4, 8, 16, 32, 64, ...)
        int powerOf2 = 1;
        while (powerOf2 < workers && powerOf2 < 128) {
            powerOf2 *= 2;
        }
        
        // Choose closest power of 2
        int lowerPower = powerOf2 / 2;
        int rounded = (workers - lowerPower < powerOf2 - workers) ? lowerPower : powerOf2;
        
        // Ensure we don't go below 1
        return Math.max(1, Math.min(rounded, 128));
    }
}
