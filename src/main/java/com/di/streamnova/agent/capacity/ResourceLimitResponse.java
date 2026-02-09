package com.di.streamnova.agent.capacity;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Builder;
import lombok.Value;

/**
 * Response body when the request cannot be processed due to limited resources (e.g. 429).
 * Message and retry-after are configurable via streamnova.capacity.*
 */
@Value
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ResourceLimitResponse {
    /** Machine-readable code, e.g. RESOURCE_LIMIT */
    String code;
    /** Human-readable message (configurable). */
    String message;
    /** Suggested seconds to wait before retry (optional). */
    Integer retryAfterSeconds;
    /** For SHARDS_NOT_AVAILABLE: shards currently available. */
    Integer availableShards;
    /** For SHARDS_NOT_AVAILABLE: shards required by this request. */
    Integer requiredShards;
}
