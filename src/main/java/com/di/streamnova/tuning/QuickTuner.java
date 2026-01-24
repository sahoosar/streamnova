package com.di.streamnova.tuning;

import com.di.streamnova.config.PipelineConfigSource;
import lombok.extern.slf4j.Slf4j;

/**
 * A quick tuner that adjusts the number of shards, partitions, and fetch size factor
 * based on system properties or environment variables. It wraps a ShardPlan and modifies
 * its fetch size calculation.
 *
 * System properties / environment variables:
 *   tuner.shards            / TUNER_SHARDS            : Integer, overrides number of shards
 *   tuner.partitions        / TUNER_PARTITIONS        : Integer, overrides number of partitions
 *   tuner.fetch.factor      / TUNER_FETCH_FACTOR      : Double, scales fetch size by this factor
 */


@Slf4j
public final class QuickTuner {

    private QuickTuner(){}

    /** Apply overrides from YAML first (if present), then from -D/env, onto a base plan. */
    public static ShardPlan apply(ShardPlan base, PipelineConfigSource pipelineConfigSource) {
        // YAML (highest precedence)
        Integer yShards     = pipelineConfigSource.getShards() != null ? pipelineConfigSource.getShards() : null;
        Integer yPartitions =  pipelineConfigSource.getPartitions() != null ? pipelineConfigSource.getPartitions() : null;
        Double  yFactor     = pipelineConfigSource.getFetchFactor()  != null ? pipelineConfigSource.getFetchFactor() : null;

        // -D / Env (fallback if YAML not present)
        Integer dShards     = intProp("tuner.shards", "TUNER_SHARDS");
        Integer dPartitions = intProp("tuner.partitions", "TUNER_PARTITIONS");
        Double  dFactor     = doubleProp("tuner.fetch.factor", "TUNER_FETCH_FACTOR");

        int shards = (yShards != null) ? yShards : (dShards != null ? dShards : base.shards);
        int parts  = (yPartitions != null) ? yPartitions : (dPartitions != null ? dPartitions : base.partitions);
        Double factor = (yFactor != null) ? yFactor : dFactor;

        return new ShardPlan(
                base.environment, base.vCpus, base.jvmHeapBytes,
                shards, parts, base.connectionPoolSize,
                base.avgRowBytes, base.bandwidthBytesPerSec,
                base.rowBufferFraction, base.targetBatchSeconds,
                base.minFetch, base.maxFetch,
                factor
        );
    }

    // existing helper overload if you want to pass factor explicitly
    public static ShardPlan apply(ShardPlan base, Double fetchFactor) {
        return new ShardPlan(
                base.environment, base.vCpus, base.jvmHeapBytes,
                base.shards, base.partitions, base.connectionPoolSize,
                base.avgRowBytes, base.bandwidthBytesPerSec,
                base.rowBufferFraction, base.targetBatchSeconds,
                base.minFetch, base.maxFetch,
                fetchFactor
        );
    }

    private static Integer intProp(String sys, String env) {
        String s = System.getProperty(sys); if (s == null) s = System.getenv(env);
        if (s == null || s.isBlank()) return null;
        try { return Integer.valueOf(s.trim()); } catch (Exception ignore) { return null; }
    }

    private static Double doubleProp(String sys, String env) {
        String s = System.getProperty(sys); if (s == null) s = System.getenv(env);
        if (s == null || s.isBlank()) return null;
        try { return Double.valueOf(s.trim()); } catch (Exception ignore) { return null; }
    }

}






