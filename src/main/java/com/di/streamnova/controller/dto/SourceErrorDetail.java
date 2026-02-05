package com.di.streamnova.controller.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * Production-style error detail for pipeline/source operations.
 * Returned when a supported source type is configured in properties but not available or fails (e.g. not implemented, config missing, connection error).
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SourceErrorDetail {

    /** Machine-readable error code for clients and monitoring. */
    private String code;

    /** Short user-facing message. */
    private String message;

    /** Source key this error refers to (e.g. "postgres", "oracle"). */
    private String sourceKey;

    /** Optional detail (e.g. exception message, hint). Not exposed for sensitive data. */
    private String detail;

    /** ISO-8601 timestamp when the error was determined. */
    private String timestamp;

    public static SourceErrorDetail of(String code, String message, String sourceKey) {
        return of(code, message, sourceKey, null);
    }

    public static SourceErrorDetail of(String code, String message, String sourceKey, String detail) {
        return SourceErrorDetail.builder()
                .code(code)
                .message(message)
                .sourceKey(sourceKey)
                .detail(detail)
                .timestamp(Instant.now().toString())
                .build();
    }

    /** Error codes used in pipeline/source APIs. */
    public static final class Codes {
        public static final String CONFIG_NOT_FOUND = "CONFIG_NOT_FOUND";
        public static final String STATS_NOT_IMPLEMENTED = "STATS_NOT_IMPLEMENTED";
        public static final String STATS_SOURCE_TYPE_NOT_ALLOWED = "STATS_SOURCE_TYPE_NOT_ALLOWED";
        public static final String STATS_CONNECTION_FAILED = "STATS_CONNECTION_FAILED";
        public static final String STATS_UNKNOWN_ERROR = "STATS_UNKNOWN_ERROR";
    }
}
