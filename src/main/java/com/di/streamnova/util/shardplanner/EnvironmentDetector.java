package com.di.streamnova.util.shardplanner;

import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;

/**
 * Detects the execution environment (machine type, vCPUs, workers).
 */
@Slf4j
public final class EnvironmentDetector {
    private EnvironmentDetector() {}
    
    public static ExecutionEnvironment detectEnvironment(PipelineOptions pipelineOptions, String userProvidedMachineType) {
        // PRIORITY 1: User-provided machine type from config (highest priority)
        String machineType = userProvidedMachineType != null && !userProvidedMachineType.isBlank() 
                ? userProvidedMachineType 
                : detectMachineType(pipelineOptions);
        
        int virtualCpus = detectVirtualCpus(pipelineOptions, machineType);
        int workerCount = detectWorkerCount(pipelineOptions);
        
        // Ensure worker count is never 0 for local execution (machineType is null/blank)
        boolean isLocal = (machineType == null || machineType.isBlank());
        if (isLocal && workerCount == 0) {
            int fallbackWorkers = Math.max(1, Runtime.getRuntime().availableProcessors());
            log.warn("Worker count was 0 for local execution, overriding to {} (available CPU cores)", fallbackWorkers);
            workerCount = fallbackWorkers;
        }
        
        return new ExecutionEnvironment(machineType, virtualCpus, workerCount);
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
    
    public static void logEnvironmentDetection(ExecutionEnvironment environment) {
        log.info("Detected machine type: {}", environment.machineType);
        log.info("Detected vCPUs: {}", environment.virtualCpus);
        log.info("Detected workers: {}", environment.workerCount);
    }
}
