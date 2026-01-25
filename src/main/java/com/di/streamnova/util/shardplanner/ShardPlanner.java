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
     * Generic method: Calculates optimal shard and worker plan for ANY deployment environment.
     * 
     * <p><b>Key Features:</b></p>
     * <ul>
     *   <li><b>Environment-Agnostic:</b> Works seamlessly across Local, GCP, AWS, and other environments</li>
     *   <li><b>Zero-Config Deployment:</b> Automatically detects environment and applies appropriate defaults</li>
     *   <li><b>Smart Defaults:</b> Provides sensible defaults for all environments without manual configuration</li>
     *   <li><b>Flexible Configuration:</b> Supports config file, PipelineOptions, system properties, and environment variables</li>
     * </ul>
     * 
     * <p><b>Calculation Priority:</b></p>
     * <ol>
     *   <li>User-provided values (if within machine type limits)</li>
     *   <li>Machine type-based calculation (if machine type provided)</li>
     *   <li>Record-count-based scenarios (if machine type not provided - defaults to LOCAL)</li>
     * </ol>
     * 
     * <p><b>Automatic Environment Detection:</b></p>
     * <ul>
     *   <li>If machine type missing → automatically defaults to LOCAL (safe, easy deployment)</li>
     *   <li>If GCP detected → requires machine type (validated)</li>
     *   <li>If AWS detected → uses AWS-specific calculations (future support)</li>
     *   <li>If LOCAL → uses local CPU cores and conservative defaults</li>
     * </ul>
     * 
     * @param pipelineOptions Pipeline execution options (can be null for local execution)
     * @param databasePoolMaxSize Maximum database connection pool size (null for unlimited)
     * @param estimatedRowCount Estimated number of rows (null if unknown)
     * @param averageRowSizeBytes Average row size in bytes (null if unknown, defaults to 200)
     * @param targetMbPerShard Target MB per shard (null to use default: 200 MB)
     * @param userProvidedShardCount User-provided shard count (null or <= 0 to calculate automatically)
     * @param userProvidedWorkerCount User-provided worker count (null or <= 0 to calculate automatically)
     * @param userProvidedMachineType User-provided machine type (null or blank to auto-detect)
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
            
            // CRITICAL: Validate machine type requirement for GCP Dataflow deployments
            // Note: If machine type is missing, it's already treated as LOCAL, so this validation
            // will only apply when GCP is detected AND machine type is provided
            validateMachineTypeForGcpDeployment(environment, pipelineOptions, userProvidedMachineType);
            
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
                
                log.info("[ENV: {}] User-provided plan: {} shards, {} workers (machine type: {})", 
                        environment.cloudProvider.name(), validatedShardCount, validatedWorkerCount, environment.machineType);
                
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
                // NOTE: Calculation is DATA-DRIVEN but CONSTRAINED by machine type limits
                // Data size determines optimal shards, but machine type provides the maximum cap
                strategy = "MACHINE_TYPE";
                String envName = environment.cloudProvider.name();
                log.info("[ENV: {}] Machine type provided ({}): calculating shards and workers based on machine type. " +
                        "Shards will be calculated from data size but capped at machine type maximum.", 
                        envName, environment.machineType);
                
                // Calculate workers first (if not provided)
                if (userProvidedWorkerCount != null && userProvidedWorkerCount > 0) {
                    calculatedWorkerCount = MachineTypeResourceValidator.validateWorkerCount(
                            userProvidedWorkerCount, environment);
                    log.info("[ENV: {}] Using user-provided worker count: {} workers", envName, calculatedWorkerCount);
                } else {
                    // Calculate optimal workers based on machine type and data
                    calculatedWorkerCount = UnifiedCalculator.calculateOptimalWorkersForMachineType(
                            environment, estimatedRowCount, averageRowSizeBytes, databasePoolMaxSize);
                    log.info("[ENV: {}] Calculated optimal worker count: {} workers (based on machine type: {})", 
                            envName, calculatedWorkerCount, environment.machineType);
                }
                
                // Update environment with calculated workers
                environment = new ExecutionEnvironment(
                        environment.machineType, environment.virtualCpus, calculatedWorkerCount, 
                        environment.isLocalExecution, environment.cloudProvider);
                
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
                // NOTE: Shards are calculated based on DATA SIZE and RECORD COUNT scenarios
                // No machine type constraints, but still respects local CPU cores and database pool limits
                strategy = "RECORD_COUNT";
                String envName = environment.cloudProvider.name();
                log.info("[ENV: {}] Machine type not provided: calculating shards and workers based on record count scenarios. " +
                        "Shards will be optimized for data size and record count, without machine type constraints.", envName);
                
                // Calculate shards using record-count scenarios
                DataSizeInfo dataSizeInfo = DataSizeCalculator.calculateDataSize(estimatedRowCount, averageRowSizeBytes);
                if (dataSizeInfo.hasSizeInformation) {
                    // PATH 1: Size-based strategy (when data size info is available)
                    // Calculates: shards = total_size_mb / target_mb_per_shard
                    // Then applies scenario optimization based on record count
                    calculatedShardCount = calculateShardsUsingSizeBasedStrategy(
                            environment, dataSizeInfo, targetMbPerShard, databasePoolMaxSize, estimatedRowCount);
                } else {
                    // PATH 2: Row-count-based strategy (when data size info is NOT available)
                    // Uses profile-based calculation as base, then applies scenario optimization
                    calculatedShardCount = calculateShardsUsingRowCountBasedStrategy(
                            environment, estimatedRowCount, databasePoolMaxSize);
                }
                
                // Calculate workers based on shards
                // Formula: workers = ceil(shards / shards_per_worker)
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
                    environment.machineType, environment.virtualCpus, calculatedWorkerCount, 
                    environment.isLocalExecution, environment.cloudProvider);
            
            String envName = environment.cloudProvider.name();
            log.info("[ENV: {}] Final plan [{}]: {} shards, {} workers (machine type: {}, vCPUs: {})", 
                    envName, strategy, calculatedShardCount, calculatedWorkerCount, 
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
            String envName = (environment != null) ? environment.cloudProvider.name() : "UNKNOWN";
            log.error("[ENV: {}] Error calculating optimal shard and worker plan", envName, e);
            throw e;
        }
    }

    // ============================================================================
    // Validation Methods
    // ============================================================================
    
    /**
     * Validates that machine type is provided when deploying to GCP Dataflow.
     * 
     * <p><b>Requirement:</b> Machine type is MANDATORY for GCP Dataflow deployments to ensure:</p>
     * <ul>
     *   <li>Proper resource allocation and cost optimization</li>
     *   <li>Accurate shard and worker calculations</li>
     *   <li>Resource constraint validation</li>
     *   <li>Production-ready configuration</li>
     * </ul>
     * 
     * <p><b>Exception:</b> Local execution does not require machine type (uses local CPU cores).
     * If machine type is missing, the system automatically defaults to LOCAL execution.</p>
     * 
     * @param environment Detected execution environment
     * @param pipelineOptions Pipeline options for GCP detection
     * @param userProvidedMachineType User-provided machine type from config
     * @throws IllegalArgumentException if GCP Dataflow is detected but machine type is missing
     */
    private static void validateMachineTypeForGcpDeployment(ExecutionEnvironment environment,
                                                           PipelineOptions pipelineOptions,
                                                           String userProvidedMachineType) {
        // If environment is already LOCAL (machine type missing), skip validation
        // Missing machine type automatically defaults to LOCAL execution
            String envName = environment.cloudProvider.name();
            
            if (environment.isLocalExecution || environment.cloudProvider == ExecutionEnvironment.CloudProvider.LOCAL) {
            log.debug("[ENV: {}] Local execution detected (machine type missing) - machine type validation skipped", envName);
            return;
        }
        
        // Check if we're running on GCP Dataflow
        boolean isGcpDataflow = EnvironmentDetector.isGcpDataflow(pipelineOptions);
        
        // If GCP Dataflow is detected, machine type MUST be provided
        if (isGcpDataflow) {
            String machineType = environment.machineType;
            
            if (machineType == null || machineType.isBlank()) {
                String errorMessage = String.format(
                    "[ENV: %s] ❌ Machine type is REQUIRED for GCP Dataflow deployments.\n\n" +
                    "GCP Dataflow detected (project and region set), but machine type is missing.\n\n" +
                    "Please provide machine type in one of the following ways:\n" +
                    "1. In pipeline_config.yml:\n" +
                    "   pipeline:\n" +
                    "     config:\n" +
                    "       source:\n" +
                    "         machineType: n2-standard-4\n\n" +
                    "2. In PipelineOptions:\n" +
                    "   dataflowOptions.setWorkerMachineType(\"n2-standard-4\");\n\n" +
                    "3. Via command line:\n" +
                    "   --machineType=n2-standard-4\n\n" +
                    "Machine type is mandatory for GCP to ensure:\n" +
                    "- Proper resource allocation and cost optimization\n" +
                    "- Accurate shard and worker calculations\n" +
                    "- Resource constraint validation\n" +
                    "- Production-ready configuration\n\n" +
                    "Note: If machine type is not provided, the system will automatically default to LOCAL execution.",
                    envName
                );
                
                log.error("[ENV: {}] {}", envName, errorMessage);
                throw new IllegalArgumentException(errorMessage);
            } else {
                log.info("[ENV: {}] ✅ Machine type validation passed for GCP Dataflow: {} (provided via {})",
                        envName, machineType,
                        userProvidedMachineType != null && !userProvidedMachineType.isBlank() 
                                ? "config file" 
                                : "PipelineOptions");
            }
        } else {
            log.debug("[ENV: {}] Not GCP Dataflow - machine type validation skipped", envName);
        }
    }
    
    // ============================================================================
    // Size-Based Calculation Strategy (when data size information is available)
    // ============================================================================
    
    /**
     * Calculates shards when data size information (MB) is available.
     * 
     * <p><b>Decision Flow (NO Machine Type):</b></p>
     * <ol>
     *   <li><b>Base Calculation:</b> shards = total_size_mb / target_mb_per_shard (default: 200 MB/shard)</li>
     *   <li><b>Scenario Optimization:</b> Apply record-count-based scenarios:
     *       <ul>
     *         <li>Very Small (< 100K): 2K-5K records/shard, min 16 shards</li>
     *         <li>Small-Medium (100K-500K): 5K-10K records/shard, min 12 shards</li>
     *         <li>Medium-Small (500K-1M): 10K-20K records/shard, max 100 shards</li>
     *         <li>Medium (1M-5M): 25K-50K records/shard, max 200 shards</li>
     *         <li>Large (5M-10M): 50K-100K records/shard, max 500 shards</li>
     *       </ul>
     *   </li>
     *   <li><b>Cost Optimization:</b> Optimize for cost efficiency</li>
     *   <li><b>Constraints:</b> Apply database pool limits and local CPU constraints</li>
     *   <li><b>Rounding:</b> Round to optimal value (power-of-2 for GCP, exact for local)</li>
     * </ol>
     * 
     * <p><b>Key Principle:</b> Data size drives the calculation, then record count scenarios refine it.
     * No machine type constraints, but respects local CPU cores and database pool limits.</p>
     */
    private static int calculateShardsUsingSizeBasedStrategy(
            ExecutionEnvironment environment,
            DataSizeInfo dataSizeInfo,
            Double targetMbPerShard,
            Integer databasePoolMaxSize,
            Long estimatedRowCount) {
        
        double targetMb = (targetMbPerShard != null && targetMbPerShard > 0) 
                ? targetMbPerShard 
                : SizeBasedConfig.DEFAULT_TARGET_MB_PER_SHARD;
        
        // STEP 1: Calculate base shards from data size
        // Formula: num_shards = total_size_mb / target_mb_per_shard
        int sizeBasedShardCount = (int) Math.ceil(dataSizeInfo.totalSizeMb / targetMb);
        sizeBasedShardCount = Math.max(1, sizeBasedShardCount);
        
        String envName = environment.cloudProvider.name();
        log.info("[ENV: {}] Size-based shard calculation: {} MB total / {} MB per shard (target) = {} shards", 
                envName, String.format("%.2f", dataSizeInfo.totalSizeMb), targetMb, sizeBasedShardCount);
        
        // STEP 2: Apply optimization based on whether machine type is provided
        MachineProfile profile = MachineProfileProvider.getProfile(environment.machineType);
        int optimizedShardCount;
        
        if (environment.machineType != null && !environment.machineType.isBlank() && !environment.isLocalExecution) {
            // Machine type is provided → use machine-type-based calculation
            log.info("[ENV: {}] Machine type provided ({}): using machine-type-based optimization", envName, environment.machineType);
            optimizedShardCount = MachineTypeBasedOptimizer.optimizeBasedOnMachineType(
                    sizeBasedShardCount, estimatedRowCount, environment, profile, databasePoolMaxSize, dataSizeInfo);
        } else {
            // Machine type NOT provided → fall back to record-count-based scenarios
            log.info("[ENV: {}] Machine type not provided: applying scenario-based optimization based on record count", envName);
            optimizedShardCount = ScenarioOptimizer.optimizeForDatasetSize(
                    sizeBasedShardCount, estimatedRowCount, environment, databasePoolMaxSize);
            
            // Apply machine type adjustments (if any machine type info available, e.g., local CPU cores)
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
    // Row-Count-Based Calculation Strategy (when data size information is NOT available)
    // ============================================================================
    
    /**
     * Calculates shards when data size information (MB) is NOT available, only record count.
     * 
     * <p><b>Decision Flow (NO Machine Type):</b></p>
     * <ol>
     *   <li><b>Very Small Datasets (< 10K records):</b> Use CPU-core-based optimization
     *       <ul>
     *         <li>Formula: shards = min(total_cores, max_shards_from_data)</li>
     *         <li>Target: 50+ records per shard</li>
     *         <li>Respects: database pool limits</li>
     *       </ul>
     *   </li>
     *   <li><b>Larger Datasets (>= 10K records):</b>
     *       <ul>
     *         <li><b>Base Calculation:</b> ProfileBasedCalculator (uses vCPUs × workers × profile factors)</li>
     *         <li><b>Scenario Optimization:</b> Apply record-count-based scenarios (same as size-based strategy)</li>
     *         <li><b>Constraints:</b> Database pool limits, local CPU constraints</li>
     *       </ul>
     *   </li>
     * </ol>
     * 
     * <p><b>Key Principle:</b> When size info is unavailable, use profile-based calculation (CPU/worker-based)
     * as starting point, then refine with record-count scenarios.</p>
     */
    private static int calculateShardsUsingRowCountBasedStrategy(
            ExecutionEnvironment environment,
            Long estimatedRowCount,
            Integer databasePoolMaxSize) {
        
        String envName = environment.cloudProvider.name();
        log.info("[ENV: {}] Row-count-based calculation: data size information not available, using record count only", envName);
        
        if (estimatedRowCount != null && estimatedRowCount > 0) {
            log.info("[ENV: {}] Row count available: {} rows (record size information not available for size-based calculation)", 
                    envName, estimatedRowCount);
        } else {
            log.warn("[ENV: {}] Row count not available: {}", envName, estimatedRowCount);
        }
        
        // STEP 1: For very small datasets (< 10K), use CPU-core-based optimization
        if (estimatedRowCount != null && estimatedRowCount > 0 && estimatedRowCount < 10_000) {
            log.info("[ENV: {}] Very small dataset detected (< 10K records): using CPU-core-based optimization", envName);
            return SmallDatasetOptimizer.calculateForSmallDataset(
                    environment, estimatedRowCount, databasePoolMaxSize);
        }
        
        // STEP 2: For larger datasets, use profile-based calculation + scenario optimization
        MachineProfile profile = MachineProfileProvider.getProfile(environment.machineType);
        int shardCount;
        
        if (environment.machineType != null && !environment.machineType.isBlank() && !environment.isLocalExecution) {
            // Machine type is provided → use machine-type-based calculation
            log.info("[ENV: {}] Machine type provided ({}): using machine-type-based optimization for row-count strategy", 
                    envName, environment.machineType);
            
            // Calculate base from profile
            int profileBasedShards = ProfileBasedCalculator.calculateUsingProfile(
                    environment, profile, databasePoolMaxSize);
            
            // Use machine-type-based optimizer (primary strategy)
            DataSizeInfo emptyDataSize = new DataSizeInfo(null, null); // No size info in row-count strategy
            shardCount = MachineTypeBasedOptimizer.optimizeBasedOnMachineType(
                    profileBasedShards, estimatedRowCount, environment, profile, databasePoolMaxSize, emptyDataSize);
        } else {
            // Machine type NOT provided → use profile-based + scenario optimization
            log.info("[ENV: {}] Machine type not provided: using profile-based calculation + scenario optimization", envName);
            
            // STEP 2a: Calculate base shards using profile (vCPUs × workers × profile factors)
            shardCount = ProfileBasedCalculator.calculateUsingProfile(
                    environment, profile, databasePoolMaxSize);
            log.info("[ENV: {}] Profile-based base calculation: {} shards (from vCPUs: {}, workers: {})",
                    envName, shardCount, environment.virtualCpus, environment.workerCount);
            
            // STEP 2b: Apply scenario-based optimization (record-count scenarios)
            if (estimatedRowCount != null && estimatedRowCount > 0) {
                log.info("[ENV: {}] Applying scenario optimization based on record count: {} records", envName, estimatedRowCount);
                shardCount = ScenarioOptimizer.optimizeForDatasetSize(
                        shardCount, estimatedRowCount, environment, databasePoolMaxSize);
            }
            
            // STEP 2c: Apply machine type adjustments (if any machine type info available, e.g., local CPU cores)
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
                
                String envName = environment.cloudProvider.name();
                if (validatedWorkerCount != userProvidedWorkerCount) {
                    log.warn("[ENV: {}] User-provided worker count {} adjusted to {} based on machine type constraints", 
                            envName, userProvidedWorkerCount, validatedWorkerCount);
                } else {
                    log.info("[ENV: {}] Using user-provided worker count: {} workers (within machine type limits)", 
                            envName, validatedWorkerCount);
                }
                
                return validatedWorkerCount;
            }
            
            ExecutionEnvironment environment = EnvironmentDetector.detectEnvironment(pipelineOptions, null);
            String envName = environment.cloudProvider.name();
            
            // If workers are already specified, return them (no calculation needed)
            if (environment.workerCount > 1) {
                log.info("[ENV: {}] Worker count already specified: {} workers", envName, environment.workerCount);
                return environment.workerCount;
            }
            
            // For local execution, workers concept doesn't apply
            if (environment.isLocalExecution) {
                log.info("[ENV: {}] Local execution: worker count calculation not applicable", envName);
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
            
            log.info("[ENV: {}] Optimal worker calculation: {} shards / {} shards-per-worker = {} workers (rounded to {})",
                    envName, shards, shardsPerWorker, workersNeeded, optimalWorkers);
            log.info("[ENV: {}] Worker bounds: min={} (data), max={} (cost), final={}",
                    envName, minWorkersForData, maxWorkersForCost, optimalWorkers);
            
            return Math.max(1, optimalWorkers);
        } catch (Exception e) {
            String envName = "UNKNOWN";
            try {
                ExecutionEnvironment env = EnvironmentDetector.detectEnvironment(pipelineOptions, null);
                envName = env.cloudProvider.name();
            } catch (Exception ignored) {}
            log.error("[ENV: {}] Error calculating optimal worker count, defaulting to 1", envName, e);
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
