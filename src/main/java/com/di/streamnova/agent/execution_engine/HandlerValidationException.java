package com.di.streamnova.agent.execution_engine;

/**
 * Thrown when actions-mapping request uses a source, intermediate, or target handler
 * that is not in the available/configured handlers from pipeline-handlers.
 */
public class HandlerValidationException extends RuntimeException {

    public HandlerValidationException(String message) {
        super(message);
    }
}
