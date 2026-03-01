package com.di.streamnova.ai.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import lombok.Builder;
import lombok.Data;
import lombok.extern.jackson.Jacksonized;

/**
 * Inbound request body for AI agent chat endpoints.
 */
@Data
@Builder
@Jacksonized
public class AiChatRequest {

    /** Natural language message from the user. */
    @NotBlank(message = "message must not be blank")
    @Size(max = 8000, message = "message must not exceed 8000 characters")
    private String message;

    /**
     * Optional conversation ID for multi-turn context.
     * If null, a new conversation is started.
     */
    private String conversationId;

    /** Optional: hints for the agent, e.g. sourceType=postgres, table=orders. */
    private java.util.Map<String, String> context;
}
