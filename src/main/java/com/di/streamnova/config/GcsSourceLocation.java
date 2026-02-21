package com.di.streamnova.config;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * One GCS source location with path and file format (parquet, json, csv, etc.).
 * Used when type=gcs and multiple folders/buckets with different formats are needed.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class GcsSourceLocation {
    /** GCS path (e.g. gs://bucket/input/ or gs://bucket/prefix*.parquet). */
    private String path;
    /** File format: parquet, json, csv, avro, etc. */
    private String format;
}
