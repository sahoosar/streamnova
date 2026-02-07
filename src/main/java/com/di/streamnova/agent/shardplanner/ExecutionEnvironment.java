package com.di.streamnova.agent.shardplanner;

/**
 * Represents the execution environment (local vs cloud providers).
 */
public final class ExecutionEnvironment {
    public final String machineType;      // e.g., "n2-standard-4" or null for local
    public final int virtualCpus;         // Number of virtual CPUs
    public final int workerCount;          // Number of workers (1 for local, N for cloud)
    public final boolean isLocalExecution;
    public final CloudProvider cloudProvider;  // LOCAL, GCP, AWS, or UNKNOWN
    
    /**
     * Cloud provider enumeration.
     */
    public enum CloudProvider {
        LOCAL,      // Local execution (no cloud)
        GCP,        // Google Cloud Platform (Dataflow)
        AWS,        // Amazon Web Services (future support)
        UNKNOWN     // Unknown/undetected
    }
    
    public ExecutionEnvironment(String machineType, int virtualCpus, int workerCount) {
        this.machineType = machineType;
        this.virtualCpus = virtualCpus;
        this.workerCount = workerCount;
        this.isLocalExecution = (machineType == null || machineType.isBlank());
        this.cloudProvider = CloudProvider.LOCAL;  // Default to LOCAL
    }
    
    public ExecutionEnvironment(String machineType, int virtualCpus, int workerCount, boolean isLocalExecution) {
        this.machineType = machineType;
        this.virtualCpus = virtualCpus;
        this.workerCount = workerCount;
        this.isLocalExecution = isLocalExecution;
        this.cloudProvider = isLocalExecution ? CloudProvider.LOCAL : CloudProvider.UNKNOWN;
    }
    
    public ExecutionEnvironment(String machineType, int virtualCpus, int workerCount, 
                               boolean isLocalExecution, CloudProvider cloudProvider) {
        this.machineType = machineType;
        this.virtualCpus = virtualCpus;
        this.workerCount = workerCount;
        this.isLocalExecution = isLocalExecution;
        this.cloudProvider = cloudProvider;
    }
}
