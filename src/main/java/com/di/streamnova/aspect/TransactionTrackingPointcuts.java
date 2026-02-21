package com.di.streamnova.aspect;

import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.springframework.stereotype.Component;

/**
 * Centralised AspectJ pointcut definitions for transaction tracking.
 * Used by {@link ProjectTransactionTrackingAspect} so pointcuts are reusable,
 * testable, and documented in one place.
 */
@Aspect
@Component
public class TransactionTrackingPointcuts {

    /**
     * Every public method on a Spring REST controller (each API request).
     */
    @Pointcut("@within(org.springframework.web.bind.annotation.RestController) && execution(public * *(..))")
    public void restControllerMethods() {
    }

    /**
     * Public methods in runner, agent, and config services (pipeline/listener/execute flow).
     */
    @Pointcut("execution(public * com.di.streamnova.runner..*(..)) || "
            + "execution(public * com.di.streamnova.agent..*Service.*(..)) || "
            + "execution(public * com.di.streamnova.agent..*MappingService.*(..)) || "
            + "execution(public * com.di.streamnova.config.PipelineHandlerResolver.*(..)) || "
            + "execution(public * com.di.streamnova.config.EventConfigLoaderService.*(..)) || "
            + "execution(public * com.di.streamnova.config.PipelineTemplateService.*(..))")
    public void serviceMethods() {
    }
}
