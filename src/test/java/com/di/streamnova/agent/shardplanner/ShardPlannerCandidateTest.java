package com.di.streamnova.agent.shardplanner;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for ShardPlanner.suggestShardCountForCandidate (agent/candidate API without PipelineOptions).
 */
@DisplayName("ShardPlanner suggestShardCountForCandidate Tests")
class ShardPlannerCandidateTest {

    @Test
    @DisplayName("Returns shard count in 1-256 for valid machine type and table size")
    void validMachineTypeAndTableSize_returnsInRange() {
        int shards = ShardPlanner.suggestShardCountForCandidate(
                "n2-standard-4", 2,
                1_000_000L, 200,
                null);
        assertTrue(shards >= 1 && shards <= 256, "shards should be in [1, 256], got " + shards);
    }

    @Test
    @DisplayName("Returns fallback when machine type cannot be parsed for vCPUs")
    void invalidMachineType_returnsFallback() {
        int shards = ShardPlanner.suggestShardCountForCandidate(
                "unknown-machine", 1,
                1000L, 100,
                null);
        assertEquals(8, shards, "unknown machine type should return fallback 8");
    }

    @Test
    @DisplayName("Multiple workers with small table still get at least workerCount shards")
    void multipleWorkersSmallTable_minForParallelism() {
        int shards = ShardPlanner.suggestShardCountForCandidate(
                "n2-standard-4", 4,
                100L, 50,  // very small table
                null);
        assertTrue(shards >= 4, "4 workers should get at least 4 shards (min-for-parallelism), got " + shards);
        assertTrue(shards <= 256, "shards capped at 256, got " + shards);
    }

    @Test
    @DisplayName("Pool size null is accepted")
    void poolSizeNull_accepted() {
        int shards = ShardPlanner.suggestShardCountForCandidate(
                "n2-standard-8", 2,
                10_000_000L, 300,
                null);
        assertTrue(shards >= 1 && shards <= 256);
    }

    @Test
    @DisplayName("Pool size provided caps shards when relevant")
    void poolSizeProvided_respected() {
        int shards = ShardPlanner.suggestShardCountForCandidate(
                "n2-standard-16", 4,
                1_000_000_000L, 500,  // large table
                16);  // small pool
        assertTrue(shards >= 1 && shards <= 256);
        // ConstraintApplier caps at ~80% of pool; 16 * 0.8 = 12, so we expect shards <= 12 or thereabouts
        assertTrue(shards <= 20, "pool 16 should constrain shards");
    }
}
