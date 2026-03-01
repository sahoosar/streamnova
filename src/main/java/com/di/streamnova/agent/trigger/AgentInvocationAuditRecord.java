package com.di.streamnova.agent.trigger;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Builder;
import lombok.Value;

import java.time.Instant;

/**
 * One row from agent_invocation_audit for GET /api/agent/audit.
 */
@Value
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class AgentInvocationAuditRecord {

    long id;
    String callerAgentId;
    String runId;
    String endpoint;
    String requestSummary;
    int responseStatus;
    String responseSummary;
    Instant createdAt;
}
