package com.di.streamnova.util.shardplanner;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test cases for ShardWorkerPlan record.
 */
@DisplayName("ShardWorkerPlan Tests")
class ShardWorkerPlanTest {

    @Test
    @DisplayName("Should create ShardWorkerPlan with all fields")
    void testCreateShardWorkerPlan_AllFields() {
        ShardWorkerPlan plan = new ShardWorkerPlan(
            10, 5, "n1-standard-4", 4, "MACHINE_TYPE"
        );
        
        assertEquals(10, plan.shardCount());
        assertEquals(5, plan.workerCount());
        assertEquals("n1-standard-4", plan.machineType());
        assertEquals(4, plan.virtualCpus());
        assertEquals("MACHINE_TYPE", plan.calculationStrategy());
    }

    @Test
    @DisplayName("Should create ShardWorkerPlan with minimal fields")
    void testCreateShardWorkerPlan_MinimalFields() {
        ShardWorkerPlan plan = new ShardWorkerPlan(
            1, 1, null, 0, "RECORD_COUNT"
        );
        
        assertEquals(1, plan.shardCount());
        assertEquals(1, plan.workerCount());
        assertNull(plan.machineType());
        assertEquals(0, plan.virtualCpus());
        assertEquals("RECORD_COUNT", plan.calculationStrategy());
    }

    @Test
    @DisplayName("Should have correct toString representation")
    void testToString() {
        ShardWorkerPlan plan = new ShardWorkerPlan(
            10, 5, "n1-standard-4", 4, "MACHINE_TYPE"
        );
        
        String toString = plan.toString();
        assertNotNull(toString);
        assertTrue(toString.contains("10"));
        assertTrue(toString.contains("5"));
        assertTrue(toString.contains("n1-standard-4"));
    }

    @Test
    @DisplayName("Should be equal when all fields match")
    void testEquals_SameValues() {
        ShardWorkerPlan plan1 = new ShardWorkerPlan(10, 5, "n1-standard-4", 4, "MACHINE_TYPE");
        ShardWorkerPlan plan2 = new ShardWorkerPlan(10, 5, "n1-standard-4", 4, "MACHINE_TYPE");
        
        assertEquals(plan1, plan2);
        assertEquals(plan1.hashCode(), plan2.hashCode());
    }

    @Test
    @DisplayName("Should not be equal when fields differ")
    void testEquals_DifferentValues() {
        ShardWorkerPlan plan1 = new ShardWorkerPlan(10, 5, "n1-standard-4", 4, "MACHINE_TYPE");
        ShardWorkerPlan plan2 = new ShardWorkerPlan(20, 5, "n1-standard-4", 4, "MACHINE_TYPE");
        
        assertNotEquals(plan1, plan2);
    }

    @Test
    @DisplayName("Should create plan using factory methods")
    void testFactoryMethods() {
        ShardWorkerPlan userPlan = ShardWorkerPlan.userProvided(10, 5, "n1-standard-4", 4);
        assertEquals("USER_PROVIDED", userPlan.calculationStrategy());
        
        ShardWorkerPlan machinePlan = ShardWorkerPlan.machineTypeBased(10, 5, "n1-standard-4", 4);
        assertEquals("MACHINE_TYPE", machinePlan.calculationStrategy());
        
        ShardWorkerPlan recordPlan = ShardWorkerPlan.recordCountBased(10, 5, "n1-standard-4", 4);
        assertEquals("RECORD_COUNT", recordPlan.calculationStrategy());
    }
}
