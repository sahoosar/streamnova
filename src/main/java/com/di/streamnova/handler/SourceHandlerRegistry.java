package com.di.streamnova.handler;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Registry for managing and retrieving source handlers.
 * 
 * <p>This registry automatically discovers all {@link SourceHandler} beans and registers them
 * by their type. Handlers are normalized (case-insensitive, trimmed) for lookup.
 * 
 * <p>Features:
 * <ul>
 *   <li>Automatic discovery of all SourceHandler beans via Spring dependency injection</li>
 *   <li>Case-insensitive handler type lookup</li>
 *   <li>Duplicate handler detection with configurable behavior</li>
 *   <li>Type-safe handler retrieval</li>
 * </ul>
 * 
 * @author StreamNova
 * @since 1.0
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SourceHandlerRegistry {

    private final List<SourceHandler<?>> handlers;

    private Map<String, SourceHandler<?>> handlersByType;

    /**
     * Configuration flag: if true, allows duplicate handler types and keeps the first one.
     * If false (default), throws an exception when duplicate types are detected.
     */
    private static final boolean ALLOW_DUPLICATE_HANDLERS = false;

    /**
     * Initializes the registry by discovering, validating, and registering all source handlers.
     * This method is called automatically after dependency injection via {@link PostConstruct}.
     */
    @PostConstruct
    void initialize() {
        if (handlers == null || handlers.isEmpty()) {
            log.warn("No SourceHandler beans found. Registry will be empty.");
            handlersByType = Collections.emptyMap();
            return;
        }

        log.info("Discovering {} SourceHandler bean(s)...", handlers.size());
        logDiscoveredHandlers();

        Map<String, List<SourceHandler<?>>> groupedHandlers = groupHandlersByType();
        validateNoDuplicates(groupedHandlers);
        handlersByType = buildHandlerMap(groupedHandlers);

        log.info("Successfully registered {} handler type(s): {}", 
                handlersByType.size(), handlersByType.keySet());
    }

    /**
     * Retrieves the handler for the given source type. The pipeline is not fixed to one handler:
     * config supplies the type (postgres, oracle, gcs, etc.) and this returns the matching handler
     * (PostgresHandler, OracleHandler, GCSHandler, etc.) so execution runs accordingly.
     *
     * @param type the source type from pipeline config (case-insensitive)
     * @param <T> the configuration type expected by the handler
     * @return the handler for that type
     * @throws IllegalArgumentException if no handler is found for the given type
     */
    @SuppressWarnings("unchecked")
    public <T> SourceHandler<T> getHandler(String type) {
        if (type == null || type.isBlank()) {
            throw new IllegalArgumentException("Handler type cannot be null or blank");
        }

        String normalizedType = normalizeType(type);
        SourceHandler<?> handler = handlersByType.get(normalizedType);
        
        if (handler == null) {
            String availableTypes = handlersByType.keySet().toString();
            throw new IllegalArgumentException(
                    String.format("Unsupported source type: '%s'. Available types: %s", 
                            type, availableTypes));
        }
        
        return (SourceHandler<T>) handler;
    }

    /**
     * Returns all registered handler types.
     * 
     * @return an unmodifiable set of registered handler type names
     */
    public Set<String> getRegisteredTypes() {
        return Collections.unmodifiableSet(handlersByType.keySet());
    }

    /**
     * Checks if a handler is registered for the given type.
     *
     * @param type the source type to check
     * @return true if a handler is registered for this type
     */
    public boolean hasHandler(String type) {
        if (type == null || type.isBlank()) {
            return false;
        }
        return handlersByType.containsKey(normalizeType(type));
    }

    /**
     * Returns the handler class name (e.g. PostgresHandler, GCSHandler) for the given type, for logging.
     * Enables automatic detection: config says type=postgres → this returns PostgresHandler.
     *
     * @param type the source type (e.g. postgres, oracle, gcs)
     * @return the simple class name of the handler for that type, or empty if no handler registered
     */
    public Optional<String> getHandlerClassName(String type) {
        if (type == null || type.isBlank()) {
            return Optional.empty();
        }
        SourceHandler<?> handler = handlersByType.get(normalizeType(type));
        return handler == null ? Optional.empty() : Optional.of(handler.getClass().getSimpleName());
    }

    /**
     * Logs information about all discovered handlers.
     */
    private void logDiscoveredHandlers() {
        handlers.forEach(handler -> {
            String handlerType = handler.type();
            String className = handler.getClass().getName();
            int hashCode = System.identityHashCode(handler);
            log.info("  - Handler: {}@{} (type='{}')", 
                    className, 
                    Integer.toHexString(hashCode), 
                    formatTypeForLogging(handlerType));
        });
    }

    /**
     * Groups handlers by their normalized type.
     * 
     * @return a map of normalized type to list of handlers
     * @throws IllegalStateException if any handler returns a blank type
     */
    private Map<String, List<SourceHandler<?>>> groupHandlersByType() {
        return handlers.stream()
                .peek(this::validateHandlerType)
                .collect(Collectors.groupingBy(handler -> normalizeType(handler.type())));
    }

    /**
     * Validates that a handler returns a non-blank type.
     * 
     * @param handler the handler to validate
     * @throws IllegalStateException if the handler type is blank
     */
    private void validateHandlerType(SourceHandler<?> handler) {
        String handlerType = handler.type();
        if (handlerType == null || handlerType.isBlank()) {
            String className = handler.getClass().getName();
            throw new IllegalStateException(
                    String.format("Handler %s returned blank type(). Handler type must be non-null and non-blank.", 
                            className));
        }
    }

    /**
     * Validates that there are no duplicate handler types, or handles them according to configuration.
     * 
     * @param groupedHandlers the handlers grouped by type
     * @throws IllegalStateException if duplicates are found and ALLOW_DUPLICATE_HANDLERS is false
     */
    private void validateNoDuplicates(Map<String, List<SourceHandler<?>>> groupedHandlers) {
        List<Map.Entry<String, List<SourceHandler<?>>>> duplicates = findDuplicates(groupedHandlers);

        if (duplicates.isEmpty()) {
            return;
        }

        String errorMessage = formatDuplicateErrorMessage(duplicates);

        if (ALLOW_DUPLICATE_HANDLERS) {
            log.warn("{} — keeping the first handler for each type.", errorMessage);
        } else {
            throw new IllegalStateException(errorMessage);
        }
    }

    /**
     * Finds duplicate handler types.
     * 
     * @param groupedHandlers the handlers grouped by type
     * @return list of entries where multiple handlers exist for the same type
     */
    private List<Map.Entry<String, List<SourceHandler<?>>>> findDuplicates(
            Map<String, List<SourceHandler<?>>> groupedHandlers) {
        return groupedHandlers.entrySet().stream()
                .filter(entry -> entry.getValue().size() > 1)
                .toList();
    }

    /**
     * Formats an error message for duplicate handler types.
     * 
     * @param duplicates the duplicate entries
     * @return formatted error message
     */
    private String formatDuplicateErrorMessage(List<Map.Entry<String, List<SourceHandler<?>>>> duplicates) {
        String detail = duplicates.stream()
                .map(entry -> {
                    String type = entry.getKey();
                    String handlerClasses = entry.getValue().stream()
                            .map(handler -> handler.getClass().getName())
                            .collect(Collectors.joining(", "));
                    return String.format("'%s' -> [%s]", type, handlerClasses);
                })
                .collect(Collectors.joining(" ; "));
        
        return String.format("Duplicate SourceHandler type() values detected: %s", detail);
    }

    /**
     * Builds the final handler map from grouped handlers.
     * 
     * @param groupedHandlers handlers grouped by type
     * @return unmodifiable map of type to handler (first handler wins if duplicates exist)
     */
    private Map<String, SourceHandler<?>> buildHandlerMap(
            Map<String, List<SourceHandler<?>>> groupedHandlers) {
        return groupedHandlers.entrySet().stream()
                .collect(Collectors.toUnmodifiableMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue().get(0) // First handler wins if duplicates exist
                ));
    }

    /**
     * Normalizes a handler type string for consistent lookup.
     * Converts to lowercase and trims whitespace.
     * 
     * @param type the handler type to normalize
     * @return normalized type, or null if input is null
     */
    private static String normalizeType(String type) {
        return type == null ? null : type.trim().toLowerCase(Locale.ROOT);
    }

    /**
     * Formats a handler type for logging purposes.
     * 
     * @param type the handler type
     * @return formatted string for logging (handles null)
     */
    private static String formatTypeForLogging(String type) {
        return type == null ? "<null>" : type;
    }
}
