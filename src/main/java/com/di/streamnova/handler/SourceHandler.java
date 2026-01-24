package com.di.streamnova.handler;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

import com.di.streamnova.handler.config.SourceConfig;

/**
 * Interface for data source handlers
 */
public interface SourceHandler<T> {
    /** The source type key used in YAML, e.g. "Postgres" */
    String type();

    /** Do the work */
    PCollection<Row> read(Pipeline pipeline, T config);
}
