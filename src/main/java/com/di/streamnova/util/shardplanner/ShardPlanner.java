package com.di.streamnova.util.shardplanner;

import com.di.streamnova.util.ConnectionPoolLogger;
import com.di.streamnova.util.InputValidator;
import com.di.streamnova.util.MetricsCollector;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.options.PipelineOptions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Calculates optimal number of shards and workers for parallel data processing.
 */
@Slf4j
@Component
public final class ShardPlanner {

    private static volatile MetricsCollector metricsCollector;

    @Autowired(required = false)
    public void setMetricsCollector(MetricsCollector metricsCollector) {
        ShardPlanner.metricsCollector = metricsCollector;
    }

    private ShardPlanner() {}

    /**
     * Calculates optimal shard and worker plan.
     */
    public static ShardWorkerPlan calculateOptimalShardWorkerPlan(
            PipelineOptions pipelineOptions,
            Integer databasePoolMaxSize,
            Integer databaseMinimumIdle,
            Long estimatedRowCount,
            Integer averageRowSizeBytes,
            Double targetMbPerShard,
            Integer userProvidedShardCount,
            Integer userProvidedWorkerCount,
            String userProvidedMachineType) {

        long startTime = System.currentTimeMillis();
        ExecutionEnvironment environment = null;

        try {
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

            environment = EnvironmentDetector.detectEnvironment(pipelineOptions, userProvidedMachineType);
            validateMachineTypeForGcpDeployment(environment, pipelineOptions, userProvidedMachineType);

            // User provided both shards and workers
            if (userProvidedShardCount != null && userProvidedShardCount > 0
                    && userProvidedWorkerCount != null && userProvidedWorkerCount > 0) {
                MachineProfile profile = MachineProfileProvider.getProfile(environment.machineType);
                int validatedWorkerCount = MachineTypeResourceValidator.validateWorkerCount(
                        userProvidedWorkerCount, environment);
                int validatedShardCount = MachineTypeResourceValidator.validateShardCount(
                        userProvidedShardCount, environment, profile);
                validatedShardCount = ConstraintApplier.applyConstraints(
                        validatedShardCount, environment, profile, databasePoolMaxSize);
                validatedShardCount = ShardCountRounder.roundToOptimalValue(validatedShardCount, environment);

                log.info("[ENV: {}] User-provided plan: {} shards, {} workers",
                        environment.cloudProvider.name(), validatedShardCount, validatedWorkerCount);
                if (metricsCollector != null) {
                    metricsCollector.recordShardPlanningWithContext(
                            validatedShardCount, System.currentTimeMillis() - startTime,
                            environment.virtualCpus, validatedWorkerCount, estimatedRowCount);
                }
                return ShardWorkerPlan.userProvided(
                        validatedShardCount, validatedWorkerCount,
                        environment.machineType, environment.virtualCpus);
            }

            int calculatedShardCount;
            int calculatedWorkerCount;
            String strategy;

            if (environment.machineType != null && !environment.machineType.isBlank() && !environment.isLocalExecution) {
                strategy = "MACHINE_TYPE";
                String envName = environment.cloudProvider.name();
                log.info("[ENV: {}] Machine type provided ({}): calculating based on machine type", envName, environment.machineType);

                if (userProvidedWorkerCount != null && userProvidedWorkerCount > 0) {
                    calculatedWorkerCount = MachineTypeResourceValidator.validateWorkerCount(
                            userProvidedWorkerCount, environment);
                } else {
                    calculatedWorkerCount = UnifiedCalculator.calculateOptimalWorkersForMachineType(
                            environment, estimatedRowCount, averageRowSizeBytes, databasePoolMaxSize, null);
                }

                environment = new ExecutionEnvironment(
                        environment.machineType, environment.virtualCpus, calculatedWorkerCount,
                        environment.isLocalExecution, environment.cloudProvider);

                if (userProvidedShardCount != null && userProvidedShardCount > 0) {
                    MachineProfile profile = MachineProfileProvider.getProfile(environment.machineType);
                    calculatedShardCount = MachineTypeResourceValidator.validateShardCount(
                            userProvidedShardCount, environment, profile);
                    calculatedShardCount = ConstraintApplier.applyConstraints(
                            calculatedShardCount, environment, profile, databasePoolMaxSize);
                } else {
                    calculatedShardCount = UnifiedCalculator.calculateOptimalShardsForMachineType(
                            environment, estimatedRowCount, averageRowSizeBytes,
                            targetMbPerShard, databasePoolMaxSize);
                }
            } else {
                strategy = "RECORD_COUNT";
                String envName = environment.cloudProvider.name();
                log.info("[ENV: {}] Machine type not provided: calculating based on record count scenarios", envName);

                DataSizeInfo dataSizeInfo = DataSizeCalculator.calculateDataSize(estimatedRowCount, averageRowSizeBytes);
                if (dataSizeInfo.hasSizeInformation) {
                    calculatedShardCount = calculateShardsUsingSizeBasedStrategy(
                            environment, dataSizeInfo, targetMbPerShard, databasePoolMaxSize, estimatedRowCount);
                } else {
                    calculatedShardCount = calculateShardsUsingRowCountBasedStrategy(
                            environment, estimatedRowCount, databasePoolMaxSize);
                }

                if (userProvidedWorkerCount != null && userProvidedWorkerCount > 0) {
                    calculatedWorkerCount = userProvidedWorkerCount;
                } else {
                    calculatedWorkerCount = UnifiedCalculator.calculateWorkersFromShards(
                            calculatedShardCount, environment);
                }
            }

            calculatedShardCount = InputValidator.validateShardCount(calculatedShardCount);
            MachineProfile profile = MachineProfileProvider.getProfile(environment.machineType);
            calculatedShardCount = ConstraintApplier.applyConstraints(
                    calculatedShardCount, environment, profile, databasePoolMaxSize);
            calculatedShardCount = ShardCountRounder.roundToOptimalValue(calculatedShardCount, environment);

            environment = new ExecutionEnvironment(
                    environment.machineType, environment.virtualCpus, calculatedWorkerCount,
                    environment.isLocalExecution, environment.cloudProvider);

            String envName = environment.cloudProvider.name();
            log.info("[ENV: {}] Final plan [{}]: {} shards, {} workers",
                    envName, strategy, calculatedShardCount, calculatedWorkerCount);

            int poolMax = (databasePoolMaxSize != null && databasePoolMaxSize > 0) ? databasePoolMaxSize : 16;
            int minIdle = (databaseMinimumIdle != null && databaseMinimumIdle > 0)
                    ? databaseMinimumIdle : Math.min(2, poolMax);
            ConnectionPoolLogger.logStartupSummary(poolMax, minIdle, calculatedShardCount, calculatedWorkerCount);

            if (metricsCollector != null) {
                metricsCollector.recordShardPlanningWithContext(
                        calculatedShardCount, System.currentTimeMillis() - startTime,
                        environment.virtualCpus, calculatedWorkerCount, estimatedRowCount);
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

    private static void validateMachineTypeForGcpDeployment(ExecutionEnvironment environment,
                                                           PipelineOptions pipelineOptions,
                                                           String userProvidedMachineType) {
        if (environment.isLocalExecution || environment.cloudProvider == ExecutionEnvironment.CloudProvider.LOCAL) {
            return;
        }
        if (EnvironmentDetector.isGcpDataflow(pipelineOptions)) {
            if (environment.machineType == null || environment.machineType.isBlank()) {
                throw new IllegalArgumentException(
                        "[ENV: GCP] Machine type is REQUIRED for GCP Dataflow. Please provide machineType in pipeline_config.yml.");
            }
        }
    }

    private static int calculateShardsUsingSizeBasedStrategy(
            ExecutionEnvironment environment,
            DataSizeInfo dataSizeInfo,
            Double targetMbPerShard,
            Integer databasePoolMaxSize,
            Long estimatedRowCount) {

        double targetMb = (targetMbPerShard != null && targetMbPerShard > 0)
                ? targetMbPerShard
                : SizeBasedConfig.DEFAULT_TARGET_MB_PER_SHARD;
        int sizeBasedShardCount = (int) Math.ceil(dataSizeInfo.totalSizeMb / targetMb);
        sizeBasedShardCount = Math.max(1, sizeBasedShardCount);

        MachineProfile profile = MachineProfileProvider.getProfile(environment.machineType);
        int optimizedShardCount;
        if (environment.machineType != null && !environment.machineType.isBlank() && !environment.isLocalExecution) {
            DataSizeInfo info = new DataSizeInfo(dataSizeInfo.totalSizeMb, dataSizeInfo.totalSizeBytes);
            optimizedShardCount = MachineTypeBasedOptimizer.optimizeBasedOnMachineType(
                    sizeBasedShardCount, estimatedRowCount, environment, profile, databasePoolMaxSize, info);
        } else {
            optimizedShardCount = ScenarioOptimizer.optimizeForDatasetSize(
                    sizeBasedShardCount, estimatedRowCount, environment, databasePoolMaxSize);
            optimizedShardCount = MachineTypeAdjuster.adjustForMachineType(optimizedShardCount, environment, profile);
        }

        optimizedShardCount = CostOptimizer.optimizeForCost(optimizedShardCount, environment, estimatedRowCount);
        optimizedShardCount = ConstraintApplier.applyConstraints(
                optimizedShardCount, environment, profile, databasePoolMaxSize);
        return ShardCountRounder.roundToOptimalValue(optimizedShardCount, environment);
    }

    private static int calculateShardsUsingRowCountBasedStrategy(
            ExecutionEnvironment environment,
            Long estimatedRowCount,
            Integer databasePoolMaxSize) {

        MachineProfile profile = MachineProfileProvider.getProfile(environment.machineType);
        int shardCount;
        if (estimatedRowCount != null && estimatedRowCount > 0 && estimatedRowCount < 10_000) {
            shardCount = SmallDatasetOptimizer.calculateForSmallDataset(
                    environment, estimatedRowCount, databasePoolMaxSize);
        } else {
            shardCount = ProfileBasedCalculator.calculateUsingProfile(environment, profile, databasePoolMaxSize);
            if (estimatedRowCount != null && estimatedRowCount > 0) {
                shardCount = ScenarioOptimizer.optimizeForDatasetSize(
                        shardCount, estimatedRowCount, environment, databasePoolMaxSize);
            }
            shardCount = MachineTypeAdjuster.adjustForMachineType(shardCount, environment, profile);
        }

        shardCount = CostOptimizer.optimizeForCost(shardCount, environment, estimatedRowCount);
        shardCount = ConstraintApplier.applyConstraints(shardCount, environment, profile, databasePoolMaxSize);
        return ShardCountRounder.roundToOptimalValue(shardCount, environment);
    }
}
