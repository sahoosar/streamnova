package com.di.streamnova.util;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sample;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.schemas.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.*;
import org.apache.beam.sdk.values.TypeDescriptors;
import java.util.Objects;
import org.slf4j.MDC;

/**
 * Utility class for row-level logging of DLQ (Dead Letter Queue) rows.
 * 
 * NOTE: This class is currently UNUSED in production code.
 * It was used in the example method checkUsingLogging() which is commented out.
 * 
 * If you need row-level logging for debugging data quality issues:
 * 1. Uncomment checkUsingLogging() in DataflowRunnerService
 * 2. Uncomment DLQ logger in logback-spring.xml
 * 3. Use DlqLogging methods to log individual error rows
 * 
 * WARNING: Row-level logging can generate millions of log entries.
 * Use only for debugging, not in production.
 */
public final class DlqLogging {

    private DlqLogging() {}

    /* -------------------- PASS-THROUGH: log every DLQ row -------------------- */

    /** Log every DLQ row (pass-through). */
    public static PCollection<Row> logPassThrough(PCollection<Row> dlq, String loggerName, String jobId) {
        PCollection<Row> out = dlq.apply(
                "Log DLQ rows",
                ParDo.of(new LogRowDoFn(loggerName, /*passThrough=*/true, 2000, true, jobId))
        );
        out.setRowSchema(Objects.requireNonNull(dlq.getSchema(), "DLQ must have schema"));
        return out;
    }

    /** Log every DLQ row (pass-through) with options. */
    public static PCollection<Row> logPassThrough(
            PCollection<Row> dlq, String loggerName, String jobId, int maxPayloadChars, boolean includePayload) {

        PCollection<Row> out = dlq.apply(
                "Log DLQ rows",
                ParDo.of(new LogRowDoFn(loggerName, /*passThrough=*/true, maxPayloadChars, includePayload, jobId))
        );
        out.setRowSchema(Objects.requireNonNull(dlq.getSchema(), "DLQ must have schema"));
        return out;
    }

    /* -------------------- SIDE-EFFECT ONLY: sample & counts -------------------- */

    /** Log up to N sampled rows (side-effect only), returns original dlq unchanged. */
    public static PCollection<Row> logSampleSide(PCollection<Row> dlq, int maxToLog, String loggerName, String jobId) {
        dlq.apply("Sample DLQ", Sample.any(maxToLog))
                .apply("Log sampled DLQ",
                        ParDo.of(new LogRowDoFn(loggerName, /*passThrough=*/false, 2000, true, jobId)));
        return dlq;
    }

    /** Aggregate and log counts per error_reason (window-aware). */
    public static void logCountsSide(PCollection<Row> dlq, String loggerName, String jobId) {
        dlq
                .apply("KV(reason,1)", MapElements.into(
                                TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.longs()))
                        .via(r -> {
                            Schema s = Objects.requireNonNull(r.getSchema(), "DLQ row has no schema");
                            int idx = s.indexOf("error_reason");
                            String reason = (idx >= 0) ? String.valueOf(r.getValue(idx)) : "<missing>";
                            return KV.of(reason, 1L);
                        }))
                .apply("Count per reason", Combine.perKey(Long::sum))
                .apply("Log DLQ counts", ParDo.of(new DoFn<KV<String,Long>, Void>() {
                    private transient Logger log;
                    @Setup public void setup() { log = LoggerFactory.getLogger(loggerName); }
                    @ProcessElement public void process(@Element KV<String,Long> e) {
                        try (MDC.MDCCloseable m = MDC.putCloseable("jobId", jobId)) {
                            log.warn("DLQ summary: reason={} count={}", e.getKey(), e.getValue());
                        }
                    }
                }));
    }

    /* -------------------- DoFn impl -------------------- */

    /** Internal DoFn that logs a DLQ row; pass-through or side-effect only. */
    private static final class LogRowDoFn extends DoFn<Row, Row> {
        private final String loggerName;
        private final boolean passThrough;
        private final int maxPayloadChars;
        private final boolean includePayload;
        private final String jobId;

        private transient Logger log;

        // cached field indexes (computed on first element)
        private transient boolean inited;
        private transient int reasonIdx, detailsIdx, bucketIdx, payloadIdx;

        LogRowDoFn(String loggerName, boolean passThrough, int maxPayloadChars,
                   boolean includePayload, String jobId) {
            this.loggerName = loggerName;
            this.passThrough = passThrough;
            this.maxPayloadChars = maxPayloadChars;
            this.includePayload = includePayload;
            this.jobId = jobId;
        }

        @Setup public void setup() { log = LoggerFactory.getLogger(loggerName); }

        @ProcessElement
        public void process(@Element Row r, OutputReceiver<Row> out) {
            Row row = Objects.requireNonNull(r, "DLQ row is null");
            Schema s = Objects.requireNonNull(row.getSchema(), "DLQ row has no schema");

            if (!inited) {
                reasonIdx  = s.indexOf("error_reason");
                detailsIdx = s.indexOf("error_details");
                bucketIdx  = s.hasField("shard_bucket") ? s.indexOf("shard_bucket") : -1;
                payloadIdx = s.indexOf("payload");
                inited = true;
            }

            String reason  = (reasonIdx  >= 0) ? String.valueOf(row.getValue(reasonIdx))  : "<missing>";
            String details = (detailsIdx >= 0) ? String.valueOf(row.getValue(detailsIdx)) : "<missing>";
            Object bucketObj  = (bucketIdx  >= 0) ? row.getValue(bucketIdx)                  : null;
            String bucket = (bucketObj != null) ? String.valueOf(bucketObj) : "<not_set>";

            String payloadStr = "<disabled>";
            if (includePayload && payloadIdx >= 0) {
                Object payload = row.getValue(payloadIdx);
                payloadStr = (payload != null) ? String.valueOf(payload) : "<null>";
                if (maxPayloadChars > 0 && payloadStr.length() > maxPayloadChars) {
                    payloadStr = payloadStr.substring(0, maxPayloadChars) + "...";
                }
            }

            // set MDC on this worker thread for this log call
            try (MDC.MDCCloseable m = MDC.putCloseable("jobId", jobId)) {
                log.warn("DLQ reason={}, details={}, shard_bucket={}, payload={}",
                        reason, details, bucket, payloadStr);
            }

            if (passThrough) out.output(row);
        }
    }
}


