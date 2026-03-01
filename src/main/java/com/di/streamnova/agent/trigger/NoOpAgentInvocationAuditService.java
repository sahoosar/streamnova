package com.di.streamnova.agent.trigger;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;

@Service
@ConditionalOnProperty(name = "streamnova.metrics.persistence-enabled", havingValue = "false", matchIfMissing = true)
public class NoOpAgentInvocationAuditService implements AgentInvocationAuditService {

    @Override
    public void saveWebhookInvocation(String callerAgentId, String runId, String endpoint,
                                      String requestSummary, int responseStatus, String responseSummary) {
        // no-op when persistence is disabled
    }

    @Override
    public List<AgentInvocationAuditRecord> findRecentByCallerAgentId(String callerAgentId, int limit) {
        return Collections.emptyList();
    }
}
