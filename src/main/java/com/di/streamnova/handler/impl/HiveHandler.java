package com.di.streamnova.handler.impl;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.springframework.stereotype.Component;

import com.di.streamnova.handler.SourceHandler;
/*
@Component
public class HiveHandler implements SourceHandler {
    @Override
    public boolean supports(String sourceType) {
        return "hive".equalsIgnoreCase(sourceType);
    }

    @Override
    public PCollection<Row> read(Pipeline pipeline) {
        // implement Hive read logic here
        return pipeline.apply("ReadFromHive", ...);
    }
}*/