package com.di.streamnova.aspect;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * Production-grade AspectJ aspect: tracks every transaction (controller and optionally service calls)
 * with logging, metrics, and correlation ID. Never throws; failures in logging/metrics do not affect the request.
 * <p>
 * Features:
 * <ul>
 *   <li>MDC {@code txId} for correlation across logs</li>
 *   <li>Optional Micrometer timer and counter for dashboards (when {@code metrics-enabled=true})</li>
 *   <li>Optional structured log lines (key=value) for log aggregation</li>
 *   <li>Pointcuts defined in {@link TransactionTrackingPointcuts}</li>
 * </ul>
 * For multithreaded work (e.g. {@code ExecutorService}, {@code CompletableFuture}), use
 * {@link com.di.streamnova.util.MdcPropagation#runWithTxId(Runnable)} / {@link com.di.streamnova.util.MdcPropagation#callWithTxId(java.util.concurrent.Callable)}
 * or {@link com.di.streamnova.util.MdcPropagation#wrapExecutor(java.util.concurrent.ExecutorService)} so child-thread logs carry the same {@code txId}.
 * Configure via {@code streamnova.aspect.*} in application.yml.
 */
@Slf4j
@Aspect
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class ProjectTransactionTrackingAspect {

    private static final String MDC_TX_ID = "txId";
    private static final String STATUS_SUCCESS = "success";
    private static final String STATUS_FAILURE = "failure";

    private final boolean trackControllers;
    private final boolean trackServices;
    private final boolean metricsEnabled;
    private final String metricNamePrefix;
    private final boolean structuredLogging;
    private final MeterRegistry meterRegistry;

    public ProjectTransactionTrackingAspect(
            @org.springframework.beans.factory.annotation.Value("${streamnova.aspect.track-controllers:true}") boolean trackControllers,
            @org.springframework.beans.factory.annotation.Value("${streamnova.aspect.track-services:false}") boolean trackServices,
            @org.springframework.beans.factory.annotation.Value("${streamnova.aspect.metrics-enabled:true}") boolean metricsEnabled,
            @org.springframework.beans.factory.annotation.Value("${streamnova.aspect.metric-name-prefix:streamnova.tx}") String metricNamePrefix,
            @org.springframework.beans.factory.annotation.Value("${streamnova.aspect.structured-logging:false}") boolean structuredLogging,
            @Autowired(required = false) MeterRegistry meterRegistry) {
        this.trackControllers = trackControllers;
        this.trackServices = trackServices;
        this.metricsEnabled = metricsEnabled;
        this.metricNamePrefix = metricNamePrefix != null ? metricNamePrefix : "streamnova.tx";
        this.structuredLogging = structuredLogging;
        this.meterRegistry = meterRegistry;
        log.info("[ASPECT] ProjectTransactionTrackingAspect enabled: track-controllers={}, track-services={}, metrics={}, structured-logging={}",
                trackControllers, trackServices, metricsEnabled, structuredLogging);
    }

    @Around("com.di.streamnova.aspect.TransactionTrackingPointcuts.restControllerMethods()")
    public Object trackController(ProceedingJoinPoint joinPoint) throws Throwable {
        if (!trackControllers) {
            return joinPoint.proceed();
        }
        return trackInvocation(joinPoint, "CONTROLLER");
    }

    @Around("com.di.streamnova.aspect.TransactionTrackingPointcuts.serviceMethods()")
    public Object trackService(ProceedingJoinPoint joinPoint) throws Throwable {
        if (!trackServices) {
            return joinPoint.proceed();
        }
        return trackInvocation(joinPoint, "SERVICE");
    }

    private Object trackInvocation(ProceedingJoinPoint joinPoint, String layer) throws Throwable {
        String txId = MDC.get(MDC_TX_ID);
        boolean wePutTxId = (txId == null || txId.isBlank());
        if (wePutTxId) {
            txId = "tx-" + UUID.randomUUID().toString().substring(0, 8);
            MDC.put(MDC_TX_ID, txId);
        }
        String targetClass = joinPoint.getTarget().getClass().getSimpleName();
        String methodName = joinPoint.getSignature().getName();
        if (joinPoint.getSignature() instanceof MethodSignature ms) {
            methodName = ms.getMethod().getName();
        }
        long startNanos = System.nanoTime();
        safeLogStart(layer, targetClass, methodName, txId);
        Timer.Sample sample = startTimerIfEnabled(layer);
        try {
            Object result = joinPoint.proceed();
            long durationMs = (System.nanoTime() - startNanos) / 1_000_000;
            recordSuccessIfEnabled(layer, sample, durationMs);
            safeLogEnd(layer, targetClass, methodName, txId, durationMs);
            return result;
        } catch (Throwable e) {
            long durationMs = (System.nanoTime() - startNanos) / 1_000_000;
            recordFailureIfEnabled(layer, sample, durationMs);
            safeLogFailure(layer, targetClass, methodName, txId, durationMs, e);
            throw e;
        } finally {
            if (wePutTxId) {
                MDC.remove(MDC_TX_ID);
            }
        }
    }

    private void safeLogStart(String layer, String targetClass, String methodName, String txId) {
        try {
            if (structuredLogging) {
                log.info("event=TX-START layer={} class={} method={} txId={}", layer, targetClass, methodName, txId);
            } else {
                log.info("[TX-START] {} {}.{} txId={}", layer, targetClass, methodName, txId);
            }
        } catch (Exception e) {
            log.debug("Aspect log start failed: {}", e.getMessage());
        }
    }

    private void safeLogEnd(String layer, String targetClass, String methodName, String txId, long durationMs) {
        try {
            if (structuredLogging) {
                log.info("event=TX-END layer={} class={} method={} txId={} durationMs={}", layer, targetClass, methodName, txId, durationMs);
            } else {
                log.info("[TX-END] {} {}.{} txId={} durationMs={}", layer, targetClass, methodName, txId, durationMs);
            }
        } catch (Exception e) {
            log.debug("Aspect log end failed: {}", e.getMessage());
        }
    }

    private void safeLogFailure(String layer, String targetClass, String methodName, String txId, long durationMs, Throwable e) {
        try {
            String errMsg = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
            if (structuredLogging) {
                log.warn("event=TX-FAIL layer={} class={} method={} txId={} durationMs={} error={}", layer, targetClass, methodName, txId, durationMs, errMsg);
            } else {
                log.warn("[TX-FAIL] {} {}.{} txId={} durationMs={} error={}", layer, targetClass, methodName, txId, durationMs, errMsg);
            }
        } catch (Exception ex) {
            log.debug("Aspect log failure failed: {}", ex.getMessage());
        }
    }

    private Timer.Sample startTimerIfEnabled(String layer) {
        if (!metricsEnabled || meterRegistry == null) {
            return null;
        }
        try {
            return Timer.start(meterRegistry);
        } catch (Exception e) {
            log.debug("Aspect timer start failed: {}", e.getMessage());
            return null;
        }
    }

    private void recordSuccessIfEnabled(String layer, Timer.Sample sample, long durationMs) {
        if (!metricsEnabled || meterRegistry == null) {
            return;
        }
        try {
            if (sample != null) {
                Timer timer = Timer.builder(metricNamePrefix + ".duration")
                        .description("Transaction invocation duration")
                        .tag("layer", layer)
                        .tag("status", STATUS_SUCCESS)
                        .register(meterRegistry);
                sample.stop(timer);
            }
            Counter.builder(metricNamePrefix + ".count")
                    .description("Transaction invocation count")
                    .tag("layer", layer)
                    .tag("status", STATUS_SUCCESS)
                    .register(meterRegistry)
                    .increment();
        } catch (Exception e) {
            log.debug("Aspect metrics success failed: {}", e.getMessage());
        }
    }

    private void recordFailureIfEnabled(String layer, Timer.Sample sample, long durationMs) {
        if (!metricsEnabled || meterRegistry == null) {
            return;
        }
        try {
            if (sample != null) {
                Timer timer = Timer.builder(metricNamePrefix + ".duration")
                        .description("Transaction invocation duration")
                        .tag("layer", layer)
                        .tag("status", STATUS_FAILURE)
                        .register(meterRegistry);
                sample.stop(timer);
            }
            Counter.builder(metricNamePrefix + ".count")
                    .description("Transaction invocation count")
                    .tag("layer", layer)
                    .tag("status", STATUS_FAILURE)
                    .register(meterRegistry)
                    .increment();
        } catch (Exception e) {
            log.debug("Aspect metrics failure failed: {}", e.getMessage());
        }
    }
}
