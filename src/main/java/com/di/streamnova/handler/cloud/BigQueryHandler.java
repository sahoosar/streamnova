package com.di.streamnova.handler.cloud;

import com.di.streamnova.config.PipelineConfigSource;
import com.di.streamnova.handler.SourceHandler;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.springframework.stereotype.Component;

/**
 * Source handler for reading from BigQuery (e.g. export BQ → GCS).
 * Use with source=bq and target=gcs for a 2-stage BQ → GCS pipeline.
 * Config: bq_event_config.yml pipeline.config.sources.bigquery (project, dataset, table, query, etc.).
 */
@Component
public class BigQueryHandler implements SourceHandler<PipelineConfigSource> {

    @Override
    public String type() {
        return "bigquery";
    }

    @Override
    public PCollection<Row> read(Pipeline pipeline, PipelineConfigSource config) {
        if (config == null) {
            throw new IllegalArgumentException("PipelineConfigSource is required for BigQueryHandler.");
        }
        String project = config.getProject();
        String dataset = config.getDataset();
        if (project == null || project.isBlank() || dataset == null || dataset.isBlank()) {
            throw new IllegalStateException(
                    "BigQuery source requires pipeline.config.sources.bigquery with project and dataset (and table or query) in bq_event_config.yml.");
        }
        // TODO: Implement BigQuery read (e.g. Beam BigQueryIO.readTable() or read(Query), then convert to Row).
        // For now throw so caller knows to implement; once implemented return pipeline.apply(BigQueryIO.readTable()...).
        throw new UnsupportedOperationException(
                "BigQueryHandler.read() is not yet implemented. Implement read from project='" + project + "', dataset='" + dataset + "' and return PCollection<Row>.");
    }
}
