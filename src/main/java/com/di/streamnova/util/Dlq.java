package com.di.streamnova.util;


import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.*;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.*;

/*
DLQ = Dead Letter Queue — a place/stream/table where you send records that fail validation so the pipeline can keep
moving while you investigate or replay those bad rows later.

Observability: see exactly which rows failed and why.
Partial progress: good rows keep flowing; bad ones don’t block the job.
Replayability: fix the bug/data, then re-ingest DLQ rows.
 */



public final class Dlq {

    private Dlq() {}

    public static final class Result {
        public final PCollection<Row> valid;
        public final PCollection<Row> dlq;
        public final Schema payloadSchema;
        public final Schema dlqSchema;
        private Result(PCollection<Row> valid, PCollection<Row> dlq, Schema payloadSchema, Schema dlqSchema) {
            this.valid = valid; this.dlq = dlq; this.payloadSchema = payloadSchema; this.dlqSchema = dlqSchema;
        }
    }

    public static Result split(PCollection<Row> input,
                               SerializableFunction<Row, String> validator,
                               long maxBad) {

        Schema payloadSchema = input.getSchema();

        Schema dlqSchema = Schema.builder()
                .addStringField("error_reason")
                .addStringField("error_details")
                .addRowField("payload", payloadSchema)
                .build();

        TupleTag<Row> validTag = new TupleTag<Row>() {};
        TupleTag<Row> dlqTag   = new TupleTag<Row>() {};

        PCollectionTuple out = input.apply("Validate & Split",
                ParDo.of(new DoFn<Row, Row>() {
                    @ProcessElement
                    public void process(@Element Row r, MultiOutputReceiver o) {
                        String reason = validator.apply(r);
                        if (reason == null) {
                            o.get(validTag).output(r);
                        } else {
                            Row err = Row.withSchema(dlqSchema)
                                    .addValues(reason, "Row failed validation", r)
                                    .build();
                            o.get(dlqTag).output(err);
                        }
                    }
                }).withOutputTags(validTag, TupleTagList.of(dlqTag)));

        PCollection<Row> valid = out.get(validTag).setRowSchema(payloadSchema);
        PCollection<Row> dlq   = out.get(dlqTag).setRowSchema(dlqSchema);

        if (maxBad < Long.MAX_VALUE) {
            dlq.apply("Count DLQ", Count.globally())
                    .apply("Fail on DLQ threshold", MapElements.into(TypeDescriptors.longs())
                            .via(cnt -> {
                                if (cnt > maxBad) throw new IllegalStateException("DLQ rows " + cnt + " exceed limit " + maxBad);
                                return cnt;
                            }));
        }

        return new Result(valid, dlq, payloadSchema, dlqSchema);
    }
}

