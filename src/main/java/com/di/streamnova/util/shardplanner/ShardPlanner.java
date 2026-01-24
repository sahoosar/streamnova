package com.di.streamnova.util.shardplanner;

import com.di.streamnova.util.InputValidator;
import com.di.streamnova.util.MetricsCollector;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.options.PipelineOptions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * ShardPlanner - Calculates optimal number of shards for parallel data processing.
 * 
 * This class determines how many parallel shards to create based on:
 * - Dataset size (records and bytes)
 * - Hardware resources (vCPUs, workers, machine type)
 * - Cost efficiency (worker count, cloud costs)
 * - Database connection pool limits
 * 
 * The planner uses scenario-based optimization for different dataset sizes:
 * - Very Small (< 100K records): Maximum parallelism
 * - Small-Medium (100K-500K): Fast parallel processing
 * - Medium-Small (500K-1M): Balanced fast processing
 * - Medium (1M-5M): Balanced cost/performance
 * - Large (5M-10M): Cost-effective with good parallelism
 */
@Slf4j
@Component
public final class ShardPlanner {

    private static volatile MetricsCollector metricsCollector;

    @Autowired(required = false)
    public void setMetricsCollector(MetricsCollector metricsCollector) {
        ShardPlanner.metricsCollector = metricsCollector;
        log.debug("MetricsCollector initialized for ShardPlanner");
    }

    private ShardPlanner() {}

    // ============================================================================
    // Public API - Main Entry Points
    // ============================================================================
    
    /**
     * Unified method: Calculates optimal shard and worker plan based on machine type and data characteristics.
     * 
     * This is the PRIMARY method that should be used in production. It calculates both shards and workers
     * together as a cohesive unit, ensuring they are optimized based on machine type resources.
     * 
     * Priority order:
     * 1. User-provided values (if within machine type limits)
     * 2. Machine type-based calculation (if machine type provided)
     * 3. Record-count-based scenarios (if machine type not provided)
     * 
     * @param pipelineOptions Pipeline execution options
     * @param databasePoolMaxSize Maximum database connection pool size (null for unlimited)
     * @param estimatedRowCount Estimated number of rows (null if unknown)
     * @param averageRowSizeBytes Average row size in bytes (null if unknown, defaults to 200)
     * @param targetMbPerShard Target MB per shard (null to use default: 200 MB)
     * @param userProvidedShardCount User-provided shard count (null or <= 0 to calculate automatically)
     * @param userProvidedWorkerCount User-provided worker count (null or <= 0 to calculate automatically)
     * @param userProvidedMachineType User-provided machine type (null or blank to detect from PipelineOptions)
     * @return ShardWorkerPlan containing shard count, worker count, machine type, and calculation strategy
     */
    public static ShardWorkerPlan calculateOptimalShardWorkerPlan(
            PipelineOptions pipelineOptions,
            Integer databasePoolMaxSize,
            Long estimatedRowCount,
            Integer averageRowSizeBytes,
            Double targetMbPerShard,
            Integer userProvidedShardCount,
            Integer userProvidedWorkerCount,
            String userProvidedMachineType) {
        
        long startTime = System.currentTimeMillis();
        ExecutionEnvironment environment = null;
        
        try {
            // Validate inputs
            if (estimatedRowCount != null) {
                InputValidator.validateRowCount(estimatedRowCount);
            }
            if (averageRowSizeBytes != null) {
                InputValidator.validateRowSizeBytes(averageRowSizeBytes);
            }
            if (databasePoolMaxSize != null) {
                InputValidator.validatePoolSize(databasePoolMaxSize);
            }
            if (userProvidedShardCount != null && userProvidedShardCount > 0) {
                InputValidator.validateShardCount(userProvidedShardCount);
            }
            if (userProvidedWorkerCount != null && userProvidedWorkerCount > 0) {
                if (userProvidedWorkerCount > 1000) {
                    throw new IllegalArgumentException("Worker count exceeds maximum of 1000, got: " + userProvidedWorkerCount);
                }
            }
            
            // Detect execution environment (with user-provided machine type override)
            environment = EnvironmentDetector.detectEnvironment(pipelineOptions, userProvidedMachineType);
            EnvironmentDetector.logEnvironmentDetection(environment);
            
            // PRIORITY 1: If user provided both shards and workers, validate and return
            if (userProvidedShardCount != null && userProvidedShardCount > 0 
                    && userProvidedWorkerCount != null && userProvidedWorkerCount > 0) {
                
                // Validate against machine type
                MachineProfile profile = MachineProfileProvider.getProfile(environment.machineType);
                int validatedWorkerCount = MachineTypeResourceValidator.validateWorkerCount(
                        userProvidedWorkerCount, environment);
                int validatedShardCount = MachineTypeResourceValidator.validateShardCount(
                        userProvidedShardCount, environment, profile);
                
                // Apply constraints
                validatedShardCount = ConstraintApplier.applyConstraints(
                        validatedShardCount, environment, profile, databasePoolMaxSize);
                validatedShardCount = ShardCountRounder.roundToOptimalValue(validatedShardCount, environment);
                
                log.info("User-provided plan: {} shards, {} workers (machine type: {})", 
                        validatedShardCount, validatedWorkerCount, environment.machineType);
                
                // Record metrics
                if (metricsCollector != null) {
                    long duration = System.currentTimeMillis() - startTime;
                    metricsCollector.recordShardPlanningWithContext(
                            validatedShardCount, duration, environment.virtualCpus, validatedWorkerCount, estimatedRowCount);
                }
                
                return ShardWorkerPlan.userProvided(
                        validatedShardCount, validatedWorkerCount, 
                        environment.machineType, environment.virtualCpus);
            }
            
            // PRIORITY 2: Calculate based on machine type (if provided) or record count scenarios
            int calculatedShardCount;
            int calculatedWorkerCount;
            String strategy;
            
            if (environment.machineType != null && !environment.machineType.isBlank() && !environment.isLocalExecution) {
                // Machine type provided → use machine-type-based calculation
                strategy = "MACHINE_TYPE";
                log.info("Machine type provided ({}): calculating shards and workers based on machine type", 
                        environment.machineType);
                
                // Calculate workers first (if not provided)
                if (userProvidedWorkerCount != null && userProvidedWorkerCount > 0) {
                    calculatedWorkerCount = MachineTypeResourceValidator.validateWorkerCount(
                            userProvidedWorkerCount, environment);
                    log.info("Using user-provided worker count: {} workers", calculatedWorkerCount);
                } else {
                    // Calculate optimal workers based on machine type and data
                    calculatedWorkerCount = UnifiedCalculator.calculateOptimalWorkersForMachineType(
                            environment, estimatedRowCount, averageRowSizeBytes, databasePoolMaxSize);
                    log.info("Calculated optimal worker count: {} workers (based on machine type: {})", 
                            calculatedWorkerCount, environment.machineType);
                }
                
                // Update environment with calculated workers
                environment = new ExecutionEnvironment(
                        environment.machineType, environment.virtualCpus, calculatedWorkerCount, environment.isLocalExecution);
                
                // Calculate shards based on machine type
                if (userProvidedShardCount != null && userProvidedShardCount > 0) {
                    calculatedShardCount = MachineTypeResourceValidator.validateShardCount(
                            userProvidedShardCount, environment, 
                            MachineProfileProvider.getProfile(environment.machineType));
                    calculatedShardCount = ConstraintApplier.applyConstraints(
                            calculatedShardCount, environment, 
                            MachineProfileProvider.getProfile(environment.machineType), databasePoolMaxSize);
                } else {
                    calculatedShardCount = UnifiedCalculator.calculateOptimalShardsForMachineType(
                            environment, estimatedRowCount, averageRowSizeBytes, 
                            targetMbPerShard, databasePoolMaxSize);
                }
                
            } else {
                // Machine type NOT provided → use record-count-based scenarios
                strategy = "RECORD_COUNT";
                log.info("Machine type not provided: calculating shards and workers based on record count scenarios");
                
                // Calculate shards using record-count scenarios
                DataSizeInfo dataSizeInfo = DataSizeCalculator.calculateDataSize(estimatedRowCount, averageRowSizeBytes);
                if (dataSizeInfo.hasSizeInformation) {
                    calculatedShardCount = calculateShardsUsingSizeBasedStrategy(
                            environment, dataSizeInfo, targetMbPerShard, databasePoolMaxSize, estimatedRowCount);
                } else {
                    calculatedShardCount = calculateShardsUsingRowCountBasedStrategy(
                            environment, estimatedRowCount, databasePoolMaxSize);
                }
                
                // Calculate workers based on shards
                if (userProvidedWorkerCount != null && userProvidedWorkerCount > 0) {
                    calculatedWorkerCount = userProvidedWorkerCount;
                } else {
                    calculatedWorkerCount = UnifiedCalculator.calculateWorkersFromShards(
                            calculatedShardCount, environment);
                }
            }
            
            // Final validation and rounding
            calculatedShardCount = InputValidator.validateShardCount(calculatedShardCount);
            MachineProfile profile = MachineProfileProvider.getProfile(environment.machineType);
            calculatedShardCount = ConstraintApplier.applyConstraints(
                    calculatedShardCount, environment, profile, databasePoolMaxSize);
            calculatedShardCount = ShardCountRounder.roundToOptimalValue(calculatedShardCount, environment);
            
            // Update environment with final worker count
            environment = new ExecutionEnvironment(
                    environment.machineType, environment.virtualCpus, calculatedWorkerCount, environment.isLocalExecution);
            
            log.info("Final plan [{}]: {} shards, {} workers (machine type: {}, vCPUs: {})", 
                    strategy, calculatedShardCount, calculatedWorkerCount, 
                    environment.machineType != null ? environment.machineType : "local", environment.virtualCpus);
            
            // Record metrics
            if (metricsCollector != null) {
                long duration = System.currentTimeMillis() - startTime;
                metricsCollector.recordShardPlanningWithContext(
                        calculatedShardCount, duration, environment.virtualCpus, calculatedWorkerCount, estimatedRowCount);
            }
            
            return strategy.equals("MACHINE_TYPE") 
                    ? ShardWorkerPlan.machineTypeBased(
                            calculatedShardCount, calculatedWorkerCount, 
                            environment.machineType, environment.virtualCpus)
                    : ShardWorkerPlan.recordCountBased(
                            calculatedShardCount, calculatedWorkerCount, 
                            environment.machineType, environment.virtualCpus);
            
        } catch (Exception e) {
            if (metricsCollector != null) {
                metricsCollector.recordShardPlanningError();
            }
            log.error("Error calculating optimal shard and worker plan", e);
            throw e;
        }
    }

    // ============================================================================
    // Size-Based Calculation Strategy
    // ============================================================================
    
    private static int calculateShardsUsingSizeBasedStrategy(
            ExecutionEnvironment environment,
            DataSizeInfo dataSizeInfo,
            Double targetMbPerShard,
            Integer databasePoolMaxSize,
            Long estimatedRowCount) {
        
        double targetMb = (targetMbPerShard != null && targetMbPerShard > 0) 
                ? targetMbPerShard 
                : SizeBasedConfig.DEFAULT_TARGET_MB_PER_SHARD;
        
        // Calculate base shards from size: num_shards = total_size_mb / target_mb_per_shard
        int sizeBasedShardCount = (int) Math.ceil(dataSizeInfo.totalSizeMb / targetMb);
        sizeBasedShardCount = Math.max(1, sizeBasedShardCount);
        
        log.info("Size-based shard calculation: {} MB total / {} MB per shard (target) = {} shards", 
                String.format("%.2f", dataSizeInfo.totalSizeMb), targetMb, sizeBasedShardCount);
        
        // PRIORITY 1: Machine type-based optimization (if machine type is provided)
        MachineProfile profile = MachineProfileProvider.getProfile(environment.machineType);
        int optimizedShardCount;
        
        if (environment.machineType != null && !environment.machineType.isBlank() && !environment.isLocalExecution) {
            // Machine type is provided → use machine-type-based calculation
            log.info("Machine type provided ({}): using machine-type-based optimization", environment.machineType);
            optimizedShardCount = MachineTypeBasedOptimizer.optimizeBasedOnMachineType(
                    sizeBasedShardCount, estimatedRowCount, environment, profile, databasePoolMaxSize, dataSizeInfo);
        } else {
            // Machine type NOT provided → fall back to record-count-based scenarios
            log.info("Machine type not provided: falling back to record-count-based scenario optimization");
            optimizedShardCount = ScenarioOptimizer.optimizeForDatasetSize(
                    sizeBasedShardCount, estimatedRowCount, environment, databasePoolMaxSize);
            
            // Apply machine type adjustments (if any machine type info available)
            optimizedShardCount = MachineTypeAdjuster.adjustForMachineType(
                    optimizedShardCount, environment, profile);
        }
        
        // Apply cost optimization
        int costOptimizedShardCount = CostOptimizer.optimizeForCost(
                optimizedShardCount, environment, estimatedRowCount);
        
        // Apply constraints (pool size, profile bounds)
        int constrainedShardCount = ConstraintApplier.applyConstraints(
                costOptimizedShardCount, environment, profile, databasePoolMaxSize);
        
        // Final rounding (power-of-2 for GCP, exact for local)
        int finalShardCount = ShardCountRounder.roundToOptimalValue(
                constrainedShardCount, environment);
        
        // Log final results
        logFinalShardPlan(environment, dataSizeInfo, sizeBasedShardCount, finalShardCount, 
                profile, databasePoolMaxSize, targetMb, estimatedRowCount);
        
        return finalShardCount;
    }

    // ============================================================================
    // Row-Count-Based Calculation Strategy
    // ============================================================================
    
    private static int calculateShardsUsingRowCountBasedStrategy(
            ExecutionEnvironment environment,
            Long estimatedRowCount,
            Integer databasePoolMaxSize) {
        
        log.info("Falling back to row-count-based calculation (size-based calculation not available)");
        
        if (estimatedRowCount != null && estimatedRowCount > 0) {
            log.info("Row count available: {} rows (record size information not available for size-based calculation)", 
                    estimatedRowCount);
        } else {
            log.warn("Row count not available: {}", estimatedRowCount);
        }
        
        // For very small datasets, use CPU-core-based optimization
        if (estimatedRowCount != null && estimatedRowCount > 0 && estimatedRowCount < 10_000) {
            return SmallDatasetOptimizer.calculateForSmallDataset(
                    environment, estimatedRowCount, databasePoolMaxSize);
        }
        
        // PRIORITY 1: Machine type-based optimization (if machine type is provided)
        MachineProfile profile = MachineProfileProvider.getProfile(environment.machineType);
        int shardCount;
        
        if (environment.machineType != null && !environment.machineType.isBlank() && !environment.isLocalExecution) {
            // Machine type is provided → use machine-type-based calculation
            log.info("Machine type provided ({}): using machine-type-based optimization for row-count strategy", 
                    environment.machineType);
            
            // Calculate base from profile
            int profileBasedShards = ProfileBasedCalculator.calculateUsingProfile(
                    environment, profile, databasePoolMaxSize);
            
            // Use machine-type-based optimizer (primary strategy)
            DataSizeInfo emptyDataSize = new DataSizeInfo(null, null); // No size info in row-count strategy
            shardCount = MachineTypeBasedOptimizer.optimizeBasedOnMachineType(
                    profileBasedShards, estimatedRowCount, environment, profile, databasePoolMaxSize, emptyDataSize);
        } else {
            // Machine type NOT provided → fall back to record-count-based scenarios
            log.info("Machine type not provided: falling back to record-count-based scenario optimization");
            
            // Use profile-based calculation as base
            shardCount = ProfileBasedCalculator.calculateUsingProfile(
                    environment, profile, databasePoolMaxSize);
            
            // Apply scenario-based optimization (record-count scenarios)
            if (estimatedRowCount != null && estimatedRowCount > 0) {
                // Apply scenario optimization based on record count
                shardCount = ScenarioOptimizer.optimizeForDatasetSize(
                        shardCount, estimatedRowCount, environment, databasePoolMaxSize);
            }
            
            // Apply machine type adjustments (if any machine type info available)
            shardCount = MachineTypeAdjuster.adjustForMachineType(shardCount, environment, profile);
        }
        
        // Apply cost optimization
        shardCount = CostOptimizer.optimizeForCost(shardCount, environment, estimatedRowCount);
        
        // Apply constraints and rounding
        shardCount = ConstraintApplier.applyConstraints(shardCount, environment, profile, databasePoolMaxSize);
        shardCount = ShardCountRounder.roundToOptimalValue(shardCount, environment);
        
        // Log results
        logRowCountBasedPlan(environment, estimatedRowCount, shardCount, profile, databasePoolMaxSize);
        
        return shardCount;
    }

    // ============================================================================
    // Public API - Supporting Methods
    // ============================================================================

    /**
     * Calculates the number of JDBC queries per worker based on machine profile.
     * 
     * @param pipelineOptions Pipeline execution options
     * @return Number of queries per worker
     */
    public static int calculateQueriesPerWorker(PipelineOptions pipelineOptions) {
        ExecutionEnvironment environment = EnvironmentDetector.detectEnvironment(pipelineOptions, null);
        MachineProfile profile = MachineProfileProvider.getProfile(environment.machineType);
        return Math.max(1, (int) Math.ceil(environment.virtualCpus * profile.jdbcConnectionsPerVcpu()));
    }

    /**
     * Calculates active queries per worker, bounded by the connection pool size.
     * 
     * @param pipelineOptions Pipeline execution options
     * @param perWorkerPoolMax Maximum pool size per worker (null for unlimited)
     * @return Active queries per worker (capped by pool size)
     */
    public static int calculateActiveQueriesPerWorker(PipelineOptions pipelineOptions, Integer perWorkerPoolMax) {
        int queriesPerWorker = calculateQueriesPerWorker(pipelineOptions);
        int poolCapacity = (perWorkerPoolMax != null && perWorkerPoolMax > 0) ? perWorkerPoolMax : Integer.MAX_VALUE;
        return Math.max(1, Math.min(queriesPerWorker, poolCapacity));
    }

    /**
     * Calculates optimal worker count based on machine type, data size, and performance requirements.
     * 
     * This method recommends the optimal number of workers when not explicitly provided,
     * considering:
     * - Machine type (vCPUs, memory characteristics)
     * - Data size (total MB, row count)
     * - Target shard count (if calculated)
     * - Cost efficiency
     * 
     * @param pipelineOptions Pipeline execution options
     * @param estimatedRowCount Estimated number of rows (null if unknown)
     * @param averageRowSizeBytes Average row size in bytes (null if unknown)
     * @param targetShardCount Target shard count (null to calculate from data size)
     * @param databasePoolMaxSize Maximum database connection pool size (null for unlimited)
     * @return Recommended optimal worker count
     */
    public static int calculateOptimalWorkerCount(PipelineOptions pipelineOptions, 
                                                   Long estimatedRowCount,
                                                   Integer averageRowSizeBytes,
                                                   Integer targetShardCount,
                                                   Integer databasePoolMaxSize,
                                                   Integer userProvidedWorkerCount) {
        try {
            // PRIORITY 1: If user provided worker count, validate against machine type
            if (userProvidedWorkerCount != null && userProvidedWorkerCount > 0) {
                if (userProvidedWorkerCount > 1000) {
                    throw new IllegalArgumentException("Worker count exceeds maximum of 1000, got: " + userProvidedWorkerCount);
                }
                
                ExecutionEnvironment environment = EnvironmentDetector.detectEnvironment(pipelineOptions, null);
                int validatedWorkerCount = MachineTypeResourceValidator.validateWorkerCount(
                        userProvidedWorkerCount, environment);
                
                if (validatedWorkerCount != userProvidedWorkerCount) {
                    log.warn("User-provided worker count {} adjusted to {} based on machine type constraints", 
                            userProvidedWorkerCount, validatedWorkerCount);
                } else {
                    log.info("Using user-provided worker count: {} workers (within machine type limits)", validatedWorkerCount);
                }
                
                return validatedWorkerCount;
            }
            
            ExecutionEnvironment environment = EnvironmentDetector.detectEnvironment(pipelineOptions, null);
            
            // If workers are already specified, return them (no calculation needed)
            if (environment.workerCount > 1) {
                log.info("Worker count already specified: {} workers", environment.workerCount);
                return environment.workerCount;
            }
            
            // For local execution, workers concept doesn't apply
            if (environment.isLocalExecution) {
                log.info("Local execution: worker count calculation not applicable");
                return 1;
            }
            
            // Calculate target shard count if not provided
            int shards = targetShardCount != null && targetShardCount > 0 
                    ? targetShardCount 
                    : calculateOptimalShardWorkerPlan(pipelineOptions, databasePoolMaxSize, 
                            estimatedRowCount, averageRowSizeBytes, null, null, userProvidedWorkerCount, null).shardCount();
            
            // Get machine profile
            MachineProfile profile = MachineProfileProvider.getProfile(environment.machineType);
            
            // Calculate optimal shards per worker based on machine type
            int shardsPerWorker = WorkerCountCalculator.calculateOptimalShardsPerWorker(environment, profile);
            
            // Calculate workers needed: workers = ceil(shards / shards_per_worker)
            int workersNeeded = (int) Math.ceil((double) shards / shardsPerWorker);
            
            // Apply data size-based minimum workers
            int minWorkersForData = WorkerCountCalculator.calculateMinimumWorkersForData(
                    estimatedRowCount, averageRowSizeBytes, environment, profile);
            
            // Apply cost-based maximum workers
            int maxWorkersForCost = WorkerCountCalculator.calculateMaximumWorkersForCost(
                    shards, environment, profile);
            
            // Final worker count: bounded by min (data requirements) and max (cost efficiency)
            int optimalWorkers = Math.max(minWorkersForData, Math.min(workersNeeded, maxWorkersForCost));
            
            // Round to power of 2 for GCP efficiency (e.g., 1, 2, 4, 8, 16, 32)
            optimalWorkers = WorkerCountCalculator.roundToOptimalWorkerCount(optimalWorkers);
            
            log.info("Optimal worker calculation: {} shards / {} shards-per-worker = {} workers (rounded to {})",
                    shards, shardsPerWorker, workersNeeded, optimalWorkers);
            log.info("Worker bounds: min={} (data), max={} (cost), final={}",
                    minWorkersForData, maxWorkersForCost, optimalWorkers);
            
            return Math.max(1, optimalWorkers);
        } catch (Exception e) {
            log.error("Error calculating optimal worker count, defaulting to 1", e);
            return 1;
        }
    }

    // ============================================================================
    // Logging Helpers
    // ============================================================================
    
    private static void logFinalShardPlan(ExecutionEnvironment environment, DataSizeInfo dataSizeInfo,
                                         int sizeBasedShardCount, int finalShardCount,
                                         MachineProfile profile, Integer databasePoolMaxSize, 
                                         double targetMb, Long estimatedRowCount) {
        String scenarioType = determineScenarioType(estimatedRowCount);
        int maxShardsFromProfile = environment.isLocalExecution
                ? Math.max(profile.minimumShards(), environment.virtualCpus * profile.maxShardsPerVcpu())
                : Math.max(profile.minimumShards(), 
                        environment.workerCount * environment.virtualCpus * profile.maxShardsPerVcpu());
        
        log.info("Size-based shard plan [{}]: machineType={}, vCPUs={}, workers={}, totalSizeMB={}, targetMB/shard={}, " +
                "sizeBasedShards={}, profileBounds=[{},{}], poolCap={}, finalShards={}",
                scenarioType, environment.machineType, environment.virtualCpus, environment.workerCount,
                String.format("%.2f", dataSizeInfo.totalSizeMb), targetMb,
                sizeBasedShardCount, profile.minimumShards(), maxShardsFromProfile, databasePoolMaxSize, finalShardCount);
        
        if (estimatedRowCount != null && estimatedRowCount > 0) {
            double finalMbPerShard = dataSizeInfo.totalSizeMb / finalShardCount;
            long finalBytesPerShard = dataSizeInfo.totalSizeBytes / finalShardCount;
            long finalRecordsPerShard = estimatedRowCount / finalShardCount;
            long remainderRecords = estimatedRowCount % finalShardCount;
            
            log.info("Final record size distribution: {} MB per shard ({} bytes, ~{} records per shard, {} shards will have +1 record)",
                    String.format("%.2f", finalMbPerShard), finalBytesPerShard, finalRecordsPerShard, remainderRecords);
            
            // Cost analysis
            int shardsPerWorker = CostOptimizer.calculateOptimalShardsPerWorker(environment);
            int estimatedWorkersNeeded = (int) Math.ceil((double) finalShardCount / shardsPerWorker);
            logCostAnalysis(estimatedRowCount, finalShardCount, finalRecordsPerShard, 
                    estimatedWorkersNeeded, environment.workerCount, shardsPerWorker);
        }
    }
    
    private static String determineScenarioType(Long estimatedRowCount) {
        if (estimatedRowCount == null || estimatedRowCount <= 0) {
            return "UNKNOWN";
        }
        if (estimatedRowCount < DatasetScenarioConfig.VERY_SMALL_THRESHOLD) {
            return "VERY SMALL (< 100K)";
        } else if (estimatedRowCount < DatasetScenarioConfig.SMALL_MEDIUM_THRESHOLD) {
            return "SMALL-MEDIUM (100K-500K)";
        } else if (estimatedRowCount < DatasetScenarioConfig.MEDIUM_SMALL_THRESHOLD) {
            return "MEDIUM-SMALL (500K-1M)";
        } else if (estimatedRowCount < DatasetScenarioConfig.MEDIUM_THRESHOLD) {
            return "MEDIUM (1M-5M)";
        } else if (estimatedRowCount <= DatasetScenarioConfig.LARGE_THRESHOLD) {
            return "LARGE (5M-10M)";
        } else {
            return "VERY LARGE (> 10M)";
        }
    }
    
    private static void logCostAnalysis(Long estimatedRowCount, int shardCount, long recordsPerShard,
                                       int estimatedWorkersNeeded, int currentWorkers, int shardsPerWorker) {
        if (estimatedRowCount == null || estimatedRowCount <= 0) {
            return;
        }
        
        String scenarioType = determineScenarioType(estimatedRowCount);
        
        if (estimatedRowCount < DatasetScenarioConfig.VERY_SMALL_THRESHOLD) {
            log.info("Very small dataset cost analysis [{} records]: {} shards, ~{} records/shard, requires ~{} workers (current: {}) - maximum parallel processing",
                    estimatedRowCount, shardCount, recordsPerShard, estimatedWorkersNeeded, currentWorkers);
        } else if (estimatedRowCount < DatasetScenarioConfig.SMALL_MEDIUM_THRESHOLD) {
            log.info("Small-medium dataset cost analysis [{} records]: {} shards, ~{} records/shard, requires ~{} workers (current: {}) - fast parallel processing",
                    estimatedRowCount, shardCount, recordsPerShard, estimatedWorkersNeeded, currentWorkers);
        } else if (estimatedRowCount < DatasetScenarioConfig.MEDIUM_SMALL_THRESHOLD) {
            log.info("Medium-small dataset cost analysis [{} records]: {} shards, ~{} records/shard, requires ~{} workers (current: {}) - balanced fast processing",
                    estimatedRowCount, shardCount, recordsPerShard, estimatedWorkersNeeded, currentWorkers);
            if (estimatedWorkersNeeded > currentWorkers) {
                log.warn("⚠ Cost warning: {} shards would require ~{} workers (current: {}). Consider reducing to {} shards.",
                        shardCount, estimatedWorkersNeeded, currentWorkers, currentWorkers * shardsPerWorker);
            }
        } else if (estimatedRowCount < DatasetScenarioConfig.MEDIUM_THRESHOLD) {
            log.info("Medium dataset cost analysis [{} records]: {} shards, ~{} records/shard, requires ~{} workers (current: {}) - balanced cost/performance",
                    estimatedRowCount, shardCount, recordsPerShard, estimatedWorkersNeeded, currentWorkers);
            if (estimatedWorkersNeeded > currentWorkers) {
                log.warn("⚠ Cost warning: {} shards would require ~{} workers (current: {}). Consider reducing to {} shards.",
                        shardCount, estimatedWorkersNeeded, currentWorkers, currentWorkers * shardsPerWorker);
            }
        } else {
            log.info("Large dataset cost analysis [{}M records]: {} shards, ~{} records/shard, requires ~{} workers (current: {}), {} shards/worker - cost-effective processing",
                    estimatedRowCount / 1_000_000L, shardCount, recordsPerShard, estimatedWorkersNeeded, currentWorkers, shardsPerWorker);
            if (estimatedWorkersNeeded <= currentWorkers) {
                log.info("✓ Cost efficient: fits within {} workers, processing {}M records with optimal parallelism",
                        currentWorkers, estimatedRowCount / 1_000_000L);
            } else {
                log.warn("⚠ Cost warning: {} shards would require ~{} workers (current: {}). Consider reducing to {} shards for cost optimization.",
                        shardCount, estimatedWorkersNeeded, currentWorkers, currentWorkers * shardsPerWorker);
            }
        }
    }
    
    private static void logRowCountBasedPlan(ExecutionEnvironment environment, Long estimatedRowCount,
                                            int shardCount, MachineProfile profile, Integer databasePoolMaxSize) {
        int maxShardsFromProfile = environment.isLocalExecution
                ? Math.max(profile.minimumShards(), environment.virtualCpus * profile.maxShardsPerVcpu())
                : Math.max(profile.minimumShards(), 
                        environment.workerCount * environment.virtualCpus * profile.maxShardsPerVcpu());
        
        log.info("Row-count-based shard plan: machineType={}, vCPUs={}, workers={}, profileBounds=[{},{}], poolCap={}, finalShards={}",
                environment.machineType, environment.virtualCpus, environment.workerCount,
                profile.minimumShards(), maxShardsFromProfile, databasePoolMaxSize, shardCount);
        
        if (estimatedRowCount != null && estimatedRowCount > 0) {
            long recordsPerShard = estimatedRowCount / shardCount;
            long remainderRecords = estimatedRowCount % shardCount;
            log.info("Row-count-based distribution: {} records per shard ({} shards will have +1 record) - record size not available for MB calculation",
                    recordsPerShard, remainderRecords);
            
            // Cost analysis
            int shardsPerWorker = CostOptimizer.calculateOptimalShardsPerWorker(environment);
            int estimatedWorkersNeeded = (int) Math.ceil((double) shardCount / shardsPerWorker);
            logCostAnalysis(estimatedRowCount, shardCount, recordsPerShard, 
                    estimatedWorkersNeeded, environment.workerCount, shardsPerWorker);
        }
    }
}
