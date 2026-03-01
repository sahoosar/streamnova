package com.di.streamnova.config;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;
import org.slf4j.MDC;

import java.io.IOException;
import java.util.UUID;

/**
 * Sets MDC (Mapped Diagnostic Context) for every HTTP request so that logs can be traced
 * across concurrent requests and multithreading. Clears MDC in {@code finally} to avoid
 * leaking to other threads (e.g. pooled worker threads).
 * <p>
 * Sets:
 * <ul>
 *   <li>{@code requestId} – unique per request (for log aggregation / tracing)</li>
 *   <li>{@code requestPath} – request URI (used by GlobalExceptionHandler and log correlation)</li>
 * </ul>
 * Controllers and aspects may add more keys (e.g. {@code txId}, {@code caller_agent_id}, {@code execution_run_id}).
 * Ensure logback pattern includes MDC (e.g. %X{requestId} or %X) so these appear in log lines.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class MdcRequestFilter extends OncePerRequestFilter {

    private static final String REQUEST_ID = "requestId";
    private static final String REQUEST_PATH = "requestPath";

    @Override
    protected void doFilterInternal(@NonNull HttpServletRequest request,
                                    @NonNull HttpServletResponse response,
                                    @NonNull FilterChain filterChain) throws ServletException, IOException {
        String requestId = "req-" + UUID.randomUUID().toString().substring(0, 8);
        String path = request.getRequestURI();
        MDC.put(REQUEST_ID, requestId);
        MDC.put(REQUEST_PATH, path != null ? path : "");
        try {
            filterChain.doFilter(request, response);
        } finally {
            MDC.remove(REQUEST_ID);
            MDC.remove(REQUEST_PATH);
        }
    }
}
