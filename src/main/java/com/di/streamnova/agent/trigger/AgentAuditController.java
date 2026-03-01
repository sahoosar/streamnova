package com.di.streamnova.agent.trigger;

import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.slf4j.MDC;

import java.util.List;

/**
 * API to list recent agent invocation audit rows (request/response summary per webhook call).
 * Each agent sees only their own invocations via required callerAgentId.
 */
@RestController
@RequestMapping("/api/agent/audit")
@RequiredArgsConstructor
public class AgentAuditController {

    private final AgentInvocationAuditService auditService;

    /**
     * List recent audit rows for the given caller agent. callerAgentId is required so each agent sees only their invocations.
     *
     * @param callerAgentId required; e.g. agent-1. Returns only rows for this agent.
     * @param limit max number of rows (default 50, max 500).
     */
    @GetMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> listAudit(
            @RequestParam String callerAgentId,
            @RequestParam(required = false, defaultValue = "50") int limit) {
        if (callerAgentId == null || callerAgentId.isBlank()) {
            return ResponseEntity.badRequest()
                    .body("callerAgentId is required. Each agent must pass their id to list only their audit (e.g. ?callerAgentId=agent-1).");
        }
        String trimmed = callerAgentId.trim();
        MDC.put("caller_agent_id", trimmed);
        try {
            List<AgentInvocationAuditRecord> records = auditService.findRecentByCallerAgentId(trimmed, limit);
            return ResponseEntity.ok(records);
        } finally {
            MDC.remove("caller_agent_id");
        }
    }
}
