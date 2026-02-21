package com.di.streamnova.handler.cloud;

import com.di.streamnova.config.PipelineConfigSource;
import com.di.streamnova.handler.SourceHandler;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.springframework.stereotype.Component;

/**
 * Source handler for reading from GCS (e.g. Parquet/CSV files).
 * Use with source=gcs and target=bigquery for a 2-stage GCS → BQ load.
 * Config: pipeline.config.connections.gcs with type: gcs and gcsPath: gs://bucket/prefix*.parquet
 */
@Component
public class GCSHandler implements SourceHandler<PipelineConfigSource> {

    @Override
    public String type() {
        return "gcs";
    }

    @Override
    public PCollection<Row> read(Pipeline pipeline, PipelineConfigSource config) {
        if (config == null) {
            throw new IllegalArgumentException("PipelineConfigSource is required for GCSHandler.");
        }
        String gcsPath = config.getGcsPath();
        if (gcsPath == null || gcsPath.isBlank()) {
            throw new IllegalStateException("GCS source requires pipeline.config.connections.gcs.gcsPath (e.g. gs://bucket/prefix*.parquet).");
        }
        // TODO: Implement GCS read (e.g. Beam ParquetIO.readFiles() or TextIO, then convert to Row with schema).
        // For now throw so caller knows to implement; once implemented return pipeline.apply(ParquetIO.readFiles()...).apply(...) with schema.
        throw new UnsupportedOperationException(
                "GCSHandler.read() is not yet implemented. Implement read from gcsPath='" + gcsPath + "' and return PCollection<Row> with schema matching your target.");
    }
}
