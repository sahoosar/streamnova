package com.di.streamnova.agent.recommender;

/**
 * User goal for the batch load: low cost, speed, or balanced.
 * Used by the Recommender to pick the best candidate.
 */
public enum UserMode {
    /** Lowest possible cost (prefer smaller/cheaper machines, fewer workers). */
    COST_OPTIMAL,
    /** Minimum execution time (prefer more workers, larger machines). */
    FAST_LOAD,
    /** Best cost–time tradeoff (balanced scoring). */
    BALANCED
}
