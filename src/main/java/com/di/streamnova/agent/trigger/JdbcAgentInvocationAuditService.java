package com.di.streamnova.agent.trigger;

import com.di.streamnova.sql.SqlQueriesProperties;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;

@Service
@ConditionalOnProperty(name = "streamnova.metrics.persistence-enabled", havingValue = "true")
public class JdbcAgentInvocationAuditService implements AgentInvocationAuditService {

    private static final RowMapper<AgentInvocationAuditRecord> ROW_MAPPER = new RowMapper<>() {
        @Override
        public AgentInvocationAuditRecord mapRow(ResultSet rs, int rowNum) throws SQLException {
            Timestamp ts = rs.getTimestamp("created_at");
            return AgentInvocationAuditRecord.builder()
                    .id(rs.getLong("id"))
                    .callerAgentId(rs.getString("caller_agent_id"))
                    .runId(rs.getString("run_id"))
                    .endpoint(rs.getString("endpoint"))
                    .requestSummary(rs.getString("request_summary"))
                    .responseStatus(rs.getInt("response_status"))
                    .responseSummary(rs.getString("response_summary"))
                    .createdAt(ts != null ? ts.toInstant() : null)
                    .build();
        }
    };

    private final JdbcTemplate jdbc;
    private final SqlQueriesProperties sql;
    private final int maxSummaryLength;
    private final int maxEndpointLength;

    public JdbcAgentInvocationAuditService(
            JdbcTemplate jdbcTemplate,
            SqlQueriesProperties sql,
            @Value("${streamnova.metrics.audit.max-summary-length:4096}") int maxSummaryLength,
            @Value("${streamnova.metrics.audit.max-endpoint-length:256}") int maxEndpointLength) {
        this.jdbc = jdbcTemplate;
        this.sql = sql;
        this.maxSummaryLength = maxSummaryLength;
        this.maxEndpointLength = maxEndpointLength;
    }

    @Override
    public void saveWebhookInvocation(String callerAgentId, String runId, String endpoint,
                                      String requestSummary, int responseStatus, String responseSummary) {
        String caller = (callerAgentId != null && !callerAgentId.isBlank()) ? callerAgentId.trim() : "api";
        String req = truncate(requestSummary, maxSummaryLength);
        String res = truncate(responseSummary, maxSummaryLength);
        String ep = endpoint != null ? truncate(endpoint, maxEndpointLength) : "";
        try {
            jdbc.update(
                    sql.getAudit().getInsert(),
                    caller,
                    runId != null && !runId.isBlank() ? runId.trim() : null,
                    ep,
                    req,
                    responseStatus,
                    res);
        } catch (Exception e) {
            // do not fail the request if audit insert fails
            org.slf4j.LoggerFactory.getLogger(JdbcAgentInvocationAuditService.class)
                    .warn("[AUDIT] Failed to save webhook invocation: {}", e.getMessage());
        }
    }

    @Override
    public List<AgentInvocationAuditRecord> findRecentByCallerAgentId(String callerAgentId, int limit) {
        if (callerAgentId == null || callerAgentId.isBlank()) return List.of();
        int safeLimit = Math.min(Math.max(1, limit), 500);
        return jdbc.query(
                sql.getAudit().getFindRecentByCaller(),
                ROW_MAPPER,
                callerAgentId.trim(),
                safeLimit);
    }

    private static String truncate(String s, int maxLen) {
        if (s == null) return null;
        if (s.length() <= maxLen) return s;
        return s.substring(0, maxLen) + "...";
    }
}
