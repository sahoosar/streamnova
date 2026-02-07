package com.di.streamnova.agent.estimator;

/**
 * Which cap limited the effective throughput (for reporting and tuning).
 */
public enum Bottleneck {
    /** Source (e.g. Oracle) read limit. */
    SOURCE,
    /** CPU/worker capacity. */
    CPU,
    /** Sink (BQ direct or GCS+BQ) write limit. */
    SINK,
    /** Parallelism (shards/workers) – no single cap dominated. */
    PARALLELISM
}
