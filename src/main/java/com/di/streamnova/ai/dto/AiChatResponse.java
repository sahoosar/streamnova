package com.di.streamnova.ai.dto;

import lombok.Builder;
import lombok.Data;

/**
 * Outbound response from AI agent chat endpoints.
 */
@Data
@Builder
public class AiChatResponse {

    private String  conversationId;
    private String  answer;
    private String  agentName;
    private String  model;
    private long    durationMs;
    private boolean success;
    private String  errorMessage;
}
