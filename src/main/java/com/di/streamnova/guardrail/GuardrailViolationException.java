package com.di.streamnova.guardrail;

/**
 * Thrown by {@link GuardrailService#assertMachineTypeAllowed(String)} when a
 * candidate machine type violates the configured guardrail policy.
 *
 * <p>Caught by {@link com.di.streamnova.exception.GlobalExceptionHandler}
 * and returned as a 400 Bad Request with the violation message.
 */
public class GuardrailViolationException extends RuntimeException {

    public GuardrailViolationException(String message) {
        super(message);
    }
}
