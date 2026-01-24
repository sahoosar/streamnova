package com.di.streamnova.handler.bigdata;

import com.di.streamnova.config.PipelineConfigSource;
import com.di.streamnova.handler.SourceHandler;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.springframework.stereotype.Component;

@Component
public class HiveHandler implements SourceHandler<PipelineConfigSource> {

    @Override
    public String type() {
        return "hive";
    }

    @Override
    public PCollection<Row> read(Pipeline pipeline, PipelineConfigSource config) {
        // TODO: Implement Hive handler
        return null;
    }
}
