package com.di.streamnova.agent.estimator;

/**
 * How data is loaded into BigQuery: direct write or via GCS.
 */
public enum LoadPattern {
    /** Source → BigQuery direct (e.g. Postgres read → BQ insert/streaming). */
    DIRECT,
    /** Source → GCS → BigQuery load job (batch load from GCS). */
    GCS_BQ
}
