package com.di.streamnova.util.shardplanner;

/**
 * Provides machine profiles for different machine types.
 */
public final class MachineProfileProvider {
    private MachineProfileProvider() {}
    
    public static MachineProfile getProfile(String machineType) {
        String machineTypeLower = (machineType == null) ? "" : machineType.toLowerCase();
        
        if (machineTypeLower.contains("highcpu")) {
            return new MachineProfile(1.5, 2.0, 8, 2);  // More CPU per core
        } else if (machineTypeLower.contains("highmem") || machineTypeLower.contains("memory")) {
            return new MachineProfile(0.75, 2.5, 8, 1);  // Memory-optimized
        } else {
            return new MachineProfile(1.0, 2.0, 8, 1);  // Standard/default
        }
    }
}
