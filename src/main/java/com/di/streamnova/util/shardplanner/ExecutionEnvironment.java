package com.di.streamnova.util.shardplanner;

/**
 * Represents the execution environment (local vs GCP Dataflow).
 */
public final class ExecutionEnvironment {
    public final String machineType;      // e.g., "n2-standard-4" or null for local
    public final int virtualCpus;         // Number of virtual CPUs
    public final int workerCount;          // Number of workers (1 for local, N for GCP)
    public final boolean isLocalExecution;
    
    public ExecutionEnvironment(String machineType, int virtualCpus, int workerCount) {
        this.machineType = machineType;
        this.virtualCpus = virtualCpus;
        this.workerCount = workerCount;
        this.isLocalExecution = (machineType == null || machineType.isBlank());
    }
    
    public ExecutionEnvironment(String machineType, int virtualCpus, int workerCount, boolean isLocalExecution) {
        this.machineType = machineType;
        this.virtualCpus = virtualCpus;
        this.workerCount = workerCount;
        this.isLocalExecution = isLocalExecution;
    }
}
