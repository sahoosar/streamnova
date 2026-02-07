package com.di.streamnova.agent.shardplanner;

/**
 * Machine profile configuration per machine family.
 * Defines how many JDBC connections and shards per vCPU for different machine types.
 */
public record MachineProfile(
        double jdbcConnectionsPerVcpu,  // How many JDBC connections per vCPU
        double shardsPerQuery,          // How many shards per query
        int minimumShards,              // Minimum shards required
        int maxShardsPerVcpu            // Maximum shards per vCPU
) {}
