package com.di.streamnova.config;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;
import org.springframework.web.method.HandlerMethod;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * Logs which controller class and method handle each request so you can see
 * "invoked PipelineListenerController#getTemplateDetailsByParams" in the console.
 * Enable with streamnova.request-logging.enabled=true (same as RequestLoggingFilter).
 */
@Slf4j
@Component
public class HandlerInvocationLoggingInterceptor implements HandlerInterceptor, WebMvcConfigurer {

    @Value("${streamnova.request-logging.enabled:true}")
    private boolean enabled;

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        if (enabled) {
            registry.addInterceptor(this).addPathPatterns("/**");
        }
    }

    @Override
    public boolean preHandle(@NonNull HttpServletRequest request,
                            @NonNull HttpServletResponse response,
                            @NonNull Object handler) {
        if (!enabled) return true;
        if (handler instanceof HandlerMethod hm) {
            String className = hm.getBeanType().getSimpleName();
            String methodName = hm.getMethod().getName();
            log.info("[INVOKED] {}.{}", className, methodName);
        } else {
            log.info("[INVOKED] {}", handler.getClass().getSimpleName());
        }
        return true;
    }
}
