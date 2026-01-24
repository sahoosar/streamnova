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
        try {
            DataflowPipelineOptions dataflowOptions = pipelineOptions.as(DataflowPipelineOptions.class);
            Integer maxWorkers = dataflowOptions.getMaxNumWorkers();
            Integer baseWorkers = dataflowOptions.getNumWorkers();
            
            // If workers are explicitly set, use them
            if (maxWorkers != null && maxWorkers > 0) {
                return maxWorkers;
            }
            if (baseWorkers != null && baseWorkers > 0) {
                return baseWorkers;
            }
            
            // If no workers specified, return 0 to indicate "calculate automatically"
            // This will be handled by calculateOptimalWorkerCount() if needed
            log.debug("No worker count specified in PipelineOptions, will calculate automatically if needed");
            return 0;
        } catch (Exception e) {
            log.debug("Failed to detect worker count from PipelineOptions (local execution): {}", e.getMessage());
            // Local execution: workers concept doesn't apply the same way
            return 1;
        }
    }
    
    public static void logEnvironmentDetection(ExecutionEnvironment environment) {
        log.info("Detected machine type: {}", environment.machineType);
        log.info("Detected vCPUs: {}", environment.virtualCpus);
        log.info("Detected workers: {}", environment.workerCount);
    }
}
