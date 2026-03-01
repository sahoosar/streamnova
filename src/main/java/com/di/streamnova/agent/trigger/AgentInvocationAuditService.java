package com.di.streamnova.agent.trigger;

import java.util.List;

/**
 * Persists a summary of external agent request/response per invocation for tracking and support.
 * When persistence is disabled, a no-op implementation is used.
 */
public interface AgentInvocationAuditService {

    /**
     * Records one webhook trigger invocation (request + response summary). Non-blocking when called from controller.
     */
    void saveWebhookInvocation(String callerAgentId, String runId, String endpoint,
                              String requestSummary, int responseStatus, String responseSummary);

    /**
     * Returns recent audit rows for the given caller agent (for GET /api/agent/audit). Empty when persistence is disabled.
     */
    List<AgentInvocationAuditRecord> findRecentByCallerAgentId(String callerAgentId, int limit);
}
