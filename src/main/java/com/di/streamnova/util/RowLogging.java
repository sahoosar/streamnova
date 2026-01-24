package com.di.streamnova.util;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sample;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.schemas.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import java.util.Objects;

/**
 * Utility class for row-level logging of valid rows.
 * 
 * NOTE: This class is currently UNUSED in production code.
 * It was used in the example method checkUsingLogging() which is commented out.
 * 
 * If you need row-level logging for debugging data quality issues:
 * 1. Uncomment checkUsingLogging() in DataflowRunnerService
 * 2. Uncomment VALID logger in logback-spring.xml
 * 3. Use RowLogging methods to log individual rows
 * 
 * WARNING: Row-level logging can generate millions of log entries.
 * Use only for debugging, not in production.
 */
public final class RowLogging {
    private RowLogging() {}

    /** Log every row (pass-through). */
    public static PCollection<Row> logPassThrough(
            PCollection<Row> rows, String loggerName, String jobId,
            SerializableFunction<Row, String> formatter) {

        PCollection<Row> out = rows.apply("Log rows",
                ParDo.of(new FormatLogDoFn(loggerName, jobId, formatter, /*passThrough=*/true)));
        out.setRowSchema(Objects.requireNonNull(rows.getSchema(), "Rows must have schema"));
        return out;
    }

    /** Log selected fields from every row (pass-through). */
    public static PCollection<Row> logFieldsPassThrough(
            PCollection<Row> rows, String loggerName, String jobId, String... fieldNames) {

        PCollection<Row> out = rows.apply("Log selected fields",
                ParDo.of(new FormatLogDoFn(
                        loggerName, jobId, formatSelectedFields(fieldNames, 1000), /*passThrough=*/true)));
        out.setRowSchema(Objects.requireNonNull(rows.getSchema(), "Rows must have schema"));
        return out;
    }

    /** Log up to N sampled rows (side-effect only). */
    public static void logSample(
            PCollection<Row> rows, int maxToLog, String loggerName, String jobId,
            SerializableFunction<Row, String> formatter) {

        rows.apply("Sample rows", Sample.any(maxToLog))
                .apply("Log sampled rows",
                        ParDo.of(new FormatLogDoFn(loggerName, jobId, formatter, /*passThrough=*/false)));
    }

    /** Build a formatter that renders selected fields with truncation. */
    public static SerializableFunction<Row, String> formatSelectedFields(String[] names, int maxChars) {
        return (Row r) -> {
            Schema s = Objects.requireNonNull(r.getSchema(), "Row has no schema");
            StringBuilder sb = new StringBuilder("{");
            for (int i = 0; i < names.length; i++) {
                String f = names[i];
                int idx = s.indexOf(f);
                Object v = (idx >= 0) ? r.getValue(idx) : null;
                String vs;
                if (v == null) {
                    vs = (idx >= 0) ? "<null>" : "<missing>";
                } else {
                    vs = String.valueOf(v);
                    if (maxChars > 0 && vs.length() > maxChars) vs = vs.substring(0, maxChars) + "...";
                }
                if (i > 0) sb.append(", ");
                sb.append(f).append("=").append(vs);
            }
            return sb.append("}").toString();
        };
    }

    /** DoFn that logs a row; can pass-through or be side-effect only. */
    private static final class FormatLogDoFn extends DoFn<Row, Row> {
        private final String loggerName;
        private final String jobId;
        private final SerializableFunction<Row, String> fmt;
        private final boolean passThrough;
        private transient Logger log;

        FormatLogDoFn(String loggerName, String jobId,
                      SerializableFunction<Row, String> fmt, boolean passThrough) {
            this.loggerName = loggerName;
            this.jobId = jobId;
            this.fmt = fmt;
            this.passThrough = passThrough;
        }

        @Setup
        public void setup() { log = LoggerFactory.getLogger(loggerName); }

        @ProcessElement
        public void process(@Element Row r, OutputReceiver<Row> out) {
            Row row = Objects.requireNonNull(r, "Row is null");
            try (org.slf4j.MDC.MDCCloseable m = org.slf4j.MDC.putCloseable("jobId", jobId)) {
                log.info("ROW {}", fmt.apply(row));
            }
            if (passThrough) out.output(row);
        }
    }
}

