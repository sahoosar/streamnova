package com.di.streamnova.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.io.FileSystems;

import java.io.IOException;
import java.io.Reader;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.List;

public class GcsUtil {

    /**
     * Reads a BigQuery schema JSON file from a GCS path and returns a TableSchema.
     *
     * @param gcsPath Path to the JSON file (e.g., gs://your-bucket/schema/my_table.json)
     * @return TableSchema object
     * @throws IOException if file cannot be read or parsed
     */
    public static TableSchema readSchemaFromGCS(String gcsPath) throws IOException {
        ReadableByteChannel channel = FileSystems
                .open(FileSystems.matchNewResource(gcsPath, false));

        try (Reader reader = Channels.newReader(channel, "UTF-8")) {
            ObjectMapper mapper = new ObjectMapper();
            List<TableFieldSchema> fields = mapper.readValue(reader, new TypeReference<>() {});
            return new TableSchema().setFields(fields);
        }
    }
}
