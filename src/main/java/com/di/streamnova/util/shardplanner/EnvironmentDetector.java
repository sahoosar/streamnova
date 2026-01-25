package com.di.streamnova.util.shardplanner;

import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;

/**
 * Generic environment detector that automatically detects and configures the execution environment.
 * 
 * <p><b>Key Features:</b></p>
 * <ul>
 *   <li><b>Automatic Detection:</b> Detects environment (Local, GCP, AWS) without manual configuration</li>
 *   <li><b>Smart Defaults:</b> Provides sensible defaults for all environments</li>
 *   <li><b>Flexible Configuration:</b> Supports config file, PipelineOptions, system properties, and environment variables</li>
 *   <li><b>Zero-Config Deployment:</b> Works out-of-the-box with minimal configuration</li>
 *   <li><b>Environment-Agnostic:</b> Easily deployable to any environment</li>
 * </ul>
 * 
 * <p><b>Detection Priority:</b></p>
 * <ol>
 *   <li>Config file (pipeline_config.yml)</li>
 *   <li>PipelineOptions (GCP Dataflow, AWS, etc.)</li>
 *   <li>System properties / Environment variables</li>
 *   <li>Automatic detection (defaults to LOCAL)</li>
 * </ol>
 */
@Slf4j
public final class EnvironmentDetector {
    private EnvironmentDetector() {}
    
    /**
     * Detects if the pipeline is running on GCP Dataflow.
     * 
     * @param pipelineOptions Pipeline options
     * @return true if GCP Dataflow is detected (project and region are set), false otherwise
     */
    public static boolean isGcpDataflow(PipelineOptions pipelineOptions) {
        try {
            DataflowPipelineOptions dataflowOptions = pipelineOptions.as(DataflowPipelineOptions.class);
            String project = dataflowOptions.getProject();
            String region = dataflowOptions.getRegion();
            
            // If project and region are set, we're on GCP Dataflow
            return project != null && !project.isBlank() && region != null && !region.isBlank();
        } catch (Exception e) {
            log.debug("Failed to detect GCP Dataflow: {}", e.getMessage());
            return false;
        }
    }
    
    /**
     * Generic environment detection that automatically detects and configures the execution environment.
     * 
     * <p><b>Detection Strategy:</b></p>
     * <ol>
     *   <li>Detect cloud provider (GCP, AWS, or LOCAL)</li>
     *   <li>Detect machine type (from config, PipelineOptions, or auto-detect)</li>
     *   <li>If machine type missing → automatically default to LOCAL (safe default)</li>
     *   <li>Detect vCPUs (from machine type, system properties, or local CPU cores)</li>
     *   <li>Detect workers (from PipelineOptions or local CPU cores)</li>
     *   <li>Apply environment-specific defaults and validations</li>
     * </ol>
     * 
     * <p><b>Zero-Config Deployment:</b> Works out-of-the-box with sensible defaults for any environment.</p>
     * 
     * @param pipelineOptions Pipeline options (can be null for local execution)
     * @param userProvidedMachineType Machine type from config file (optional)
     * @return ExecutionEnvironment with detected/configured values
     */
    public static ExecutionEnvironment detectEnvironment(PipelineOptions pipelineOptions, String userProvidedMachineType) {
        log.info("[ENV: DETECTING] Starting generic environment detection (automatic configuration)...");
        
        // STEP 1: Detect cloud provider first (helps determine defaults)
        ExecutionEnvironment.CloudProvider cloudProvider = detectCloudProvider(pipelineOptions);
        log.info("[ENV: DETECTING] Cloud provider detected: {}", cloudProvider);
        
        // STEP 2: Detect machine type with priority order
        String machineType = detectMachineTypeWithPriority(userProvidedMachineType, pipelineOptions, cloudProvider);
        
        // STEP 3: Determine if local execution
        // CRITICAL: If machine type is not provided, automatically treat as LOCAL execution
        // This ensures safe defaults and easy deployment without configuration
        boolean machineTypeMissing = (machineType == null || machineType.isBlank());
        boolean isLocal;
        String envName;
        
        if (machineTypeMissing) {
            // No machine type provided → automatically use LOCAL execution (safe default)
            cloudProvider = ExecutionEnvironment.CloudProvider.LOCAL;
            isLocal = true;
            envName = "LOCAL";
            log.info("[ENV: {}] Machine type not provided: automatically defaulting to LOCAL execution (safe default for easy deployment)", envName);
        } else {
            // Machine type provided → use detected cloud provider
            isLocal = (cloudProvider == ExecutionEnvironment.CloudProvider.LOCAL);
            envName = cloudProvider.name();
            log.info("[ENV: {}] Machine type provided: {} (cloud provider: {})", envName, machineType, cloudProvider);
        }
        
        // STEP 4: Detect vCPUs with environment-specific defaults
        int virtualCpus = detectVirtualCpusGeneric(pipelineOptions, machineType, cloudProvider, isLocal, envName);
        
        // STEP 5: Detect workers with environment-specific defaults
        int workerCount = detectWorkerCountGeneric(pipelineOptions, cloudProvider, isLocal, virtualCpus, envName);
        
        // STEP 6: Create and log environment
        ExecutionEnvironment environment = new ExecutionEnvironment(machineType, virtualCpus, workerCount, isLocal, cloudProvider);
        logEnvironmentDetection(environment);
        
        log.info("[ENV: {}] Environment detection complete: {} environment ready for deployment", 
                envName, envName);
        
        return environment;
    }
    
    /**
     * Detects machine type with priority order for generic deployment.
     * 
     * <p><b>Priority Order:</b></p>
     * <ol>
     *   <li>User-provided from config file</li>
     *   <li>PipelineOptions (GCP Dataflow, AWS, etc.)</li>
     *   <li>System properties / Environment variables</li>
     *   <li>Auto-detect from cloud provider (if applicable)</li>
     *   <li>null (defaults to LOCAL)</li>
     * </ol>
     * 
     * @param userProvidedMachineType Machine type from config
     * @param pipelineOptions Pipeline options
     * @param cloudProvider Detected cloud provider
     * @return Machine type string or null
     */
    private static String detectMachineTypeWithPriority(String userProvidedMachineType, 
                                                       PipelineOptions pipelineOptions,
                                                       ExecutionEnvironment.CloudProvider cloudProvider) {
        String envName = cloudProvider.name();
        
        // PRIORITY 1: User-provided from config file (highest priority)
        if (userProvidedMachineType != null && !userProvidedMachineType.isBlank()) {
            log.info("[ENV: {}] Machine type from config file: {}", envName, userProvidedMachineType);
            return userProvidedMachineType;
        }
        
        // PRIORITY 2: PipelineOptions (GCP Dataflow, AWS, etc.)
        String fromPipelineOptions = detectMachineType(pipelineOptions);
        if (fromPipelineOptions != null && !fromPipelineOptions.isBlank()) {
            log.info("[ENV: {}] Machine type from PipelineOptions: {}", envName, fromPipelineOptions);
            return fromPipelineOptions;
        }
        
        // PRIORITY 3: System properties / Environment variables
        String fromSystem = System.getProperty("streamnova.machine.type", System.getenv("STREAMNOVA_MACHINE_TYPE"));
        if (fromSystem != null && !fromSystem.isBlank()) {
            log.info("[ENV: {}] Machine type from system property/environment variable: {}", envName, fromSystem);
            return fromSystem;
        }
        
        // PRIORITY 4: Auto-detect from cloud provider (if applicable)
        // For GCP, we might infer from other settings, but for now return null
        // This allows the system to default to LOCAL safely
        
        // PRIORITY 5: null (will default to LOCAL)
        log.debug("[ENV: {}] Machine type not found in any source, will default to LOCAL execution", envName);
        return null;
    }
    
    /**
     * Generic vCPU detection with environment-specific defaults.
     * 
     * @param pipelineOptions Pipeline options
     * @param machineType Machine type (if available)
     * @param cloudProvider Cloud provider
     * @param isLocal Whether this is local execution
     * @param envName Environment name for logging
     * @return Detected vCPU count
     */
    private static int detectVirtualCpusGeneric(PipelineOptions pipelineOptions, 
                                               String machineType,
                                               ExecutionEnvironment.CloudProvider cloudProvider,
                                               boolean isLocal,
                                               String envName) {
        // PRIORITY 1: System property / Environment variable override
        String vCpusOverride = System.getProperty("streamnova.vcpus", System.getenv("STREAMNOVA_VCPUS"));
        if (vCpusOverride != null && !vCpusOverride.isBlank()) {
            try {
                int override = Integer.parseInt(vCpusOverride.trim());
                if (override > 0) {
                    log.info("[ENV: {}] vCPUs override: {} (from system property/environment variable)", envName, override);
                    return override;
                }
            } catch (Exception e) {
                log.debug("[ENV: {}] Failed to parse vCPU override: {}", envName, e.getMessage());
            }
        }
        
        // PRIORITY 2: Extract from machine type (if provided)
        if (machineType != null && !machineType.isBlank()) {
            int fromMachineType = extractVcpusFromMachineType(machineType);
            if (fromMachineType > 0) {
                log.info("[ENV: {}] vCPUs from machine type {}: {}", envName, machineType, fromMachineType);
                return fromMachineType;
            }
        }
        
        // PRIORITY 3: Environment-specific defaults
        if (isLocal || cloudProvider == ExecutionEnvironment.CloudProvider.LOCAL) {
            int localCpus = Math.max(1, Runtime.getRuntime().availableProcessors());
            log.info("[ENV: {}] vCPUs for LOCAL execution: {} (from available processors)", envName, localCpus);
            return localCpus;
        }
        
        // PRIORITY 4: Try to detect from PipelineOptions (legacy method for backward compatibility)
        try {
            int fromPipeline = detectVirtualCpus(pipelineOptions, machineType);
            if (fromPipeline > 0) {
                log.info("[ENV: {}] vCPUs from PipelineOptions: {}", envName, fromPipeline);
                return fromPipeline;
            }
        } catch (Exception e) {
            log.debug("[ENV: {}] Failed to detect vCPUs from PipelineOptions: {}", envName, e.getMessage());
        }
        
        // PRIORITY 5: Safe default (fallback to local CPU cores)
        int defaultCpus = Math.max(1, Runtime.getRuntime().availableProcessors());
        log.warn("[ENV: {}] Could not detect vCPUs, using default: {} (from local CPU cores)", envName, defaultCpus);
        return defaultCpus;
    }
    
    /**
     * Generic worker count detection with environment-specific defaults.
     * 
     * @param pipelineOptions Pipeline options
     * @param cloudProvider Cloud provider
     * @param isLocal Whether this is local execution
     * @param virtualCpus Detected vCPU count
     * @param envName Environment name for logging
     * @return Detected worker count
     */
    private static int detectWorkerCountGeneric(PipelineOptions pipelineOptions,
                                                ExecutionEnvironment.CloudProvider cloudProvider,
                                                boolean isLocal,
                                                int virtualCpus,
                                                String envName) {
        // PRIORITY 1: System property / Environment variable override
        String workersOverride = System.getProperty("streamnova.workers", System.getenv("STREAMNOVA_WORKERS"));
        if (workersOverride != null && !workersOverride.isBlank()) {
            try {
                int override = Integer.parseInt(workersOverride.trim());
                if (override > 0) {
                    log.info("[ENV: {}] Workers override: {} (from system property/environment variable)", envName, override);
                    return override;
                }
            } catch (Exception e) {
                log.debug("[ENV: {}] Failed to parse workers override: {}", envName, e.getMessage());
            }
        }
        
        // PRIORITY 2: Local execution defaults
        if (isLocal || cloudProvider == ExecutionEnvironment.CloudProvider.LOCAL) {
            int localWorkers = Math.max(1, virtualCpus);
            log.info("[ENV: {}] Workers for LOCAL execution: {} (based on {} vCPUs)", envName, localWorkers, virtualCpus);
            return localWorkers;
        }
        
        // PRIORITY 3: Cloud provider specific detection (legacy method for backward compatibility)
        try {
            int fromPipeline = detectWorkerCount(pipelineOptions);
            if (fromPipeline > 0) {
                log.info("[ENV: {}] Workers from PipelineOptions: {}", envName, fromPipeline);
                return fromPipeline;
            }
        } catch (Exception e) {
            log.debug("[ENV: {}] Failed to detect workers from PipelineOptions: {}", envName, e.getMessage());
        }
        
        // PRIORITY 4: Cloud provider defaults
        if (cloudProvider == ExecutionEnvironment.CloudProvider.GCP) {
            // GCP: Return 0 to indicate "calculate automatically" (GCP Dataflow will handle it)
            log.info("[ENV: {}] Workers for GCP: will be calculated automatically by Dataflow", envName);
            return 0;
        } else if (cloudProvider == ExecutionEnvironment.CloudProvider.AWS) {
            // AWS: Similar to GCP (future support)
            log.info("[ENV: {}] Workers for AWS: will be calculated automatically", envName);
            return 0;
        }
        
        // PRIORITY 5: Safe default
        int defaultWorkers = Math.max(1, virtualCpus);
        log.warn("[ENV: {}] Could not detect workers, using default: {} (based on {} vCPUs)", envName, defaultWorkers, virtualCpus);
        return defaultWorkers;
    }
    
    /**
     * Extracts vCPUs from machine type string.
     * Supports multiple formats: "n2-standard-4" -> 4, "m5.xlarge" -> 4, etc.
     * 
     * @param machineType Machine type string
     * @return vCPU count or 0 if cannot be extracted
     */
    private static int extractVcpusFromMachineType(String machineType) {
        if (machineType == null || machineType.isBlank()) {
            return 0;
        }
        
        // Try GCP format: "n2-standard-4" -> extract "4"
        String[] parts = machineType.split("-");
        if (parts.length > 0) {
            String lastPart = parts[parts.length - 1];
            try {
                int vcpus = Integer.parseInt(lastPart);
                if (vcpus > 0) {
                    return vcpus;
                }
            } catch (NumberFormatException e) {
                // Not a number, try other formats
            }
        }
        
        // Try AWS format: "m5.xlarge" -> need mapping (future support)
        // For now, return 0 to indicate cannot extract
        log.debug("Cannot extract vCPUs from machine type format: {}", machineType);
        return 0;
    }
    
    /**
     * Detects the cloud provider from PipelineOptions.
     * 
     * @param pipelineOptions Pipeline options
     * @return CloudProvider enum (LOCAL, GCP, AWS, or UNKNOWN)
     */
    public static ExecutionEnvironment.CloudProvider detectCloudProvider(PipelineOptions pipelineOptions) {
        // Check for GCP Dataflow
        if (isGcpDataflow(pipelineOptions)) {
            return ExecutionEnvironment.CloudProvider.GCP;
        }
        
        // Check for AWS (future support)
        if (isAwsExecution(pipelineOptions)) {
            return ExecutionEnvironment.CloudProvider.AWS;
        }
        
        // Default to LOCAL
        return ExecutionEnvironment.CloudProvider.LOCAL;
    }
    
    /**
     * Detects if the pipeline is running on AWS (future support).
     * 
     * @param pipelineOptions Pipeline options
     * @return true if AWS is detected, false otherwise
     */
    private static boolean isAwsExecution(PipelineOptions pipelineOptions) {
        // TODO: Implement AWS detection when AWS support is added
        // Example: Check for AWS-specific pipeline options
        // try {
        //     AwsPipelineOptions awsOptions = pipelineOptions.as(AwsPipelineOptions.class);
        //     return awsOptions.getAwsRegion() != null && !awsOptions.getAwsRegion().isBlank();
        // } catch (Exception e) {
        //     return false;
        // }
        return false;
    }
    
    private static String detectMachineType(PipelineOptions pipelineOptions) {
        try {
            return pipelineOptions.as(DataflowPipelineOptions.class).getWorkerMachineType();
        } catch (Exception e) {
            log.debug("Failed to detect machine type from PipelineOptions: {}", e.getMessage());
            return null;
        }
    }
    
    private static int detectVirtualCpus(PipelineOptions pipelineOptions, String machineType) {
        // Check for override via system property or environment variable
        String vCpusOverride = System.getProperty("tuner.vcpus", System.getenv("TUNER_VCPUS"));
        if (vCpusOverride != null && !vCpusOverride.isBlank()) {
            try {
                int override = Integer.parseInt(vCpusOverride.trim());
                if (override > 0) {
                    log.info("vCPUs override detected: {} (from system property or environment variable)", override);
                    return override;
                }
            } catch (Exception e) {
                log.debug("Failed to read vCPU override from system property: {}", e.getMessage());
                // Fall through to normal detection
            }
        }
        
        // Default: detect from available processors (for local Mac/Windows/Linux)
        int localVirtualCpus = Math.max(1, Runtime.getRuntime().availableProcessors());
        
        // If machine type provided (from config or PipelineOptions), extract vCPUs from it
        if (machineType != null && !machineType.isBlank()) {
            // Extract vCPUs from machine type (e.g., "n2-standard-4" -> 4)
            String[] parts = machineType.split("-");
            if (parts.length > 0) {
                String lastPart = parts[parts.length - 1];
                try {
                    int gcpVirtualCpus = Math.max(1, Integer.parseInt(lastPart));
                    log.info("Machine type detected: {} -> {} vCPUs", machineType, gcpVirtualCpus);
                    return gcpVirtualCpus;
                } catch (NumberFormatException e) {
                    log.warn("Cannot parse vCPUs from machine type '{}' (last part: '{}'), using local vCPUs", 
                            machineType, lastPart, e);
                }
            } else {
                log.warn("Invalid machine type format: {}, using local vCPUs", machineType);
            }
        }
        
        // Fallback: Try to get from PipelineOptions
        try {
            DataflowPipelineOptions dataflowOptions = pipelineOptions.as(DataflowPipelineOptions.class);
            String pipelineMachineType = dataflowOptions.getWorkerMachineType();
            if (pipelineMachineType != null && !pipelineMachineType.isBlank()) {
                String[] parts = pipelineMachineType.split("-");
                if (parts.length > 0) {
                    String lastPart = parts[parts.length - 1];
                    try {
                        int gcpVirtualCpus = Math.max(1, Integer.parseInt(lastPart));
                        log.info("GCP Dataflow detected: machine type {} -> {} vCPUs", pipelineMachineType, gcpVirtualCpus);
                        return gcpVirtualCpus;
                    } catch (NumberFormatException e) {
                        log.warn("Cannot parse vCPUs from machine type '{}' (last part: '{}'), using local vCPUs", 
                                pipelineMachineType, lastPart, e);
                    }
                }
            }
        } catch (Exception ignore) {
            // Ignore, will fall back to local
        }
        
        // Final fallback: Use local CPU cores
        log.info("Local execution detected: using {} vCPUs from available processors", localVirtualCpus);
        return localVirtualCpus;
    }
    
    private static int detectWorkerCount(PipelineOptions pipelineOptions) {
        // First, check if we can determine local execution by checking available processors
        int availableProcessors = Runtime.getRuntime().availableProcessors();
        if (availableProcessors <= 0) {
            log.warn("Available processors is {} (invalid), defaulting to 1 worker", availableProcessors);
            return 1;
        }
        
        // Check if we're running on GCP Dataflow or locally
        boolean isGcpDataflow = false;
        try {
            DataflowPipelineOptions dataflowOptions = pipelineOptions.as(DataflowPipelineOptions.class);
            String project = dataflowOptions.getProject();
            String region = dataflowOptions.getRegion();
            
            // If project and region are set, we're likely on GCP Dataflow
            if (project != null && !project.isBlank() && region != null && !region.isBlank()) {
                isGcpDataflow = true;
                log.debug("GCP Dataflow detected: project={}, region={}", project, region);
            } else {
                log.debug("Local execution detected: project={}, region={}", project, region);
            }
            
            // If workers are explicitly set in PipelineOptions, use them
            try {
                Integer maxWorkers = dataflowOptions.getMaxNumWorkers();
                if (maxWorkers != null && maxWorkers > 0) {
                    log.debug("Worker count from DataflowPipelineOptions.maxNumWorkers: {}", maxWorkers);
                    return maxWorkers;
                }
            } catch (Exception e) {
                log.debug("Could not read maxNumWorkers: {}", e.getMessage());
            }
            
            try {
                Integer baseWorkers = dataflowOptions.getNumWorkers();
                if (baseWorkers != null && baseWorkers > 0) {
                    log.debug("Worker count from DataflowPipelineOptions.numWorkers: {}", baseWorkers);
                    return baseWorkers;
                }
            } catch (Exception e) {
                log.debug("Could not read numWorkers: {}", e.getMessage());
            }
            
            // For GCP Dataflow: return 0 to indicate "calculate automatically"
            if (isGcpDataflow) {
                log.debug("GCP Dataflow detected: no worker count specified, will calculate automatically if needed");
                return 0;
            }
            
            // For local execution: use available CPU cores as default (never return 0)
            int localWorkers = Math.max(1, availableProcessors);
            log.info("Local execution detected: using {} workers (based on {} available CPU cores)", 
                    localWorkers, availableProcessors);
            return localWorkers;
        } catch (Exception e) {
            log.debug("Failed to detect worker count from PipelineOptions (local execution): {}", e.getMessage());
            // Local execution: use available CPU cores as default (never return 0)
            int localWorkers = Math.max(1, availableProcessors);
            log.info("Local execution detected (exception path): using {} workers (based on {} available CPU cores)", 
                    localWorkers, availableProcessors);
            return localWorkers;
        }
    }
    
    /**
     * Logs the detected environment configuration for transparency and debugging.
     * 
     * @param environment Detected execution environment
     */
    public static void logEnvironmentDetection(ExecutionEnvironment environment) {
        String envName = environment.cloudProvider.name();
        log.info("[ENV: {}] ═══════════════════════════════════════════════════════════════", envName);
        log.info("[ENV: {}] Environment Detection Summary", envName);
        log.info("[ENV: {}] ═══════════════════════════════════════════════════════════════", envName);
        log.info("[ENV: {}] Cloud Provider: {}", envName, environment.cloudProvider);
        log.info("[ENV: {}] Machine Type: {}", envName, environment.machineType != null ? environment.machineType : "N/A (LOCAL execution)");
        log.info("[ENV: {}] Virtual CPUs: {}", envName, environment.virtualCpus);
        log.info("[ENV: {}] Workers: {}", envName, environment.workerCount);
        log.info("[ENV: {}] Local Execution: {}", envName, environment.isLocalExecution);
        log.info("[ENV: {}] ═══════════════════════════════════════════════════════════════", envName);
        
        // Provide deployment guidance
        if (environment.isLocalExecution) {
            log.info("[ENV: {}] Deployment Mode: LOCAL - Optimized for local development", envName);
            log.info("[ENV: {}] Configuration: Minimal configuration required (auto-detected)", envName);
        } else {
            log.info("[ENV: {}] Deployment Mode: {} - Optimized for cloud deployment", envName, environment.cloudProvider);
            log.info("[ENV: {}] Configuration: Machine type provided, using cloud-optimized calculations", envName);
        }
    }
}
