package com.di.streamnova.config;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;
import org.springframework.web.util.ContentCachingRequestWrapper;
import org.springframework.web.util.ContentCachingResponseWrapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Enumeration;
import java.util.stream.Collectors;
import java.util.regex.Pattern;

/**
 * Logs incoming request details (method, URI, headers, body) to the console so you can see
 * what the application received. Password and auth details are never logged (redacted in body, skipped in headers).
 * Enable with streamnova.request-logging.enabled=true in application.yml.
 */
@Slf4j
@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 1)
public class RequestLoggingFilter extends OncePerRequestFilter {

    @Value("${streamnova.request-logging.enabled:true}")
    private boolean enabled;

    @Value("${streamnova.request-logging.max-body-length:2048}")
    private int maxBodyLength;

    /** Header names that contain credentials; never log their values. */
    private static final Pattern SENSITIVE_HEADER = Pattern.compile(
            "^(authorization|proxy-authorization|x-api-key|password|secret|token|credential|cookie)$", Pattern.CASE_INSENSITIVE);

    /** Redacts JSON "password" (and similar) field values. */
    private static final Pattern JSON_PASSWORD_VALUE = Pattern.compile(
            "(\"(?:password|passwd|pwd|secret|token)\"\\s*:\\s*)\"[^\"]*\"", Pattern.CASE_INSENSITIVE);
    private static final Pattern FORM_PASSWORD_VALUE = Pattern.compile(
            "(^|[?&])(password|passwd|pwd|secret|token)=[^&\\s]*", Pattern.CASE_INSENSITIVE);

    @Override
    protected void doFilterInternal(@NonNull HttpServletRequest request,
                                    @NonNull HttpServletResponse response,
                                    @NonNull FilterChain filterChain) throws ServletException, IOException {
        if (!enabled) {
            filterChain.doFilter(request, response);
            return;
        }
        ContentCachingRequestWrapper wrappedRequest = new ContentCachingRequestWrapper(request, 65536);
        ContentCachingResponseWrapper wrappedResponse = new ContentCachingResponseWrapper(response);
        try {
            filterChain.doFilter(wrappedRequest, wrappedResponse);
        } finally {
            logRequest(wrappedRequest);
            logResponse(wrappedRequest, wrappedResponse);
            wrappedResponse.copyBodyToResponse();
        }
    }

    /** Paths under which we log response body (agent endpoints) for request/response tracking. */
    private static boolean isAgentPath(String uri) {
        return uri != null && (uri.contains("/api/agent/webhook") || uri.contains("/api/agent/executions"));
    }

    private void logResponse(ContentCachingRequestWrapper request, ContentCachingResponseWrapper response) {
        try {
            int status = response.getStatus();
            String uri = request.getRequestURI();
            log.info("[RESPONSE] status={} path={}", status, uri);
            if (isAgentPath(uri)) {
                byte[] buf = response.getContentAsByteArray();
                if (buf != null && buf.length > 0) {
                    String body = new String(buf, StandardCharsets.UTF_8);
                    body = redactPasswordsFromBody(body);
                    if (body.length() > maxBodyLength) {
                        body = body.substring(0, maxBodyLength) + "... [truncated, total " + buf.length + " bytes]";
                    }
                    log.info("[RESPONSE] Body (path={}): {}", uri, body);
                }
            }
        } catch (Exception e) {
            log.warn("[RESPONSE] Could not log response: {}", e.getMessage());
        }
    }

    private void logRequest(ContentCachingRequestWrapper request) {
        try {
            String method = request.getMethod();
            String uri = request.getRequestURI();
            String query = request.getQueryString();
            String fullUri = query != null && !query.isBlank() ? uri + "?" + query : uri;
            log.info("[REQUEST] {} {}", method, fullUri);
            if (log.isDebugEnabled()) {
                Enumeration<String> headerNames = request.getHeaderNames();
                if (headerNames != null) {
                    String headers = Collections.list(headerNames).stream()
                            .filter(name -> !SENSITIVE_HEADER.matcher(name.trim()).matches())
                            .map(name -> name + "=" + request.getHeader(name))
                            .collect(Collectors.joining(", "));
                    if (!headers.isBlank()) {
                        log.debug("[REQUEST] Headers: {}", headers);
                    }
                }
            }
            byte[] buf = request.getContentAsByteArray();
            if (buf != null && buf.length > 0) {
                String contentType = request.getContentType();
                String body = new String(buf, StandardCharsets.UTF_8);
                body = redactPasswordsFromBody(body);
                if (body.length() > maxBodyLength) {
                    body = body.substring(0, maxBodyLength) + "... [truncated, total " + buf.length + " bytes]";
                }
                log.info("[REQUEST] Body (Content-Type: {}): {}", contentType != null ? contentType : "n/a", body);
            }
        } catch (Exception e) {
            log.warn("[REQUEST] Could not log body: {}", e.getMessage());
        }
    }

    /** Redacts password and other sensitive field values from body (JSON or form-style). */
    private static String redactPasswordsFromBody(String body) {
        if (body == null || body.isBlank()) return body;
        String out = JSON_PASSWORD_VALUE.matcher(body).replaceAll("$1\"***\"");
        out = FORM_PASSWORD_VALUE.matcher(out).replaceAll("$1$2=***");
        return out;
    }
}
