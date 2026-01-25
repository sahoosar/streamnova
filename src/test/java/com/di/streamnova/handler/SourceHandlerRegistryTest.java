package com.di.streamnova.handler;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.Collections;
import java.util.List;
import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test cases for SourceHandlerRegistry.
 * Note: This test manually initializes the registry without Spring context.
 */
@DisplayName("SourceHandlerRegistry Tests")
class SourceHandlerRegistryTest {

    private SourceHandlerRegistry registry;

    @BeforeEach
    void setUp() throws Exception {
        // Create registry with empty list for basic tests
        // In real scenarios, Spring will inject handlers and call @PostConstruct
        List<SourceHandler<?>> handlers = Collections.emptyList();
        registry = new SourceHandlerRegistry(handlers);
        
        // Manually call the initialize() method since @PostConstruct won't be called
        // without Spring context
        Method initializeMethod = SourceHandlerRegistry.class.getDeclaredMethod("initialize");
        initializeMethod.setAccessible(true);
        initializeMethod.invoke(registry);
    }

    @Test
    @DisplayName("Should throw exception for unsupported source type")
    void testGetHandler_UnsupportedType() {
        assertThrows(IllegalArgumentException.class, 
            () -> registry.getHandler("unsupported"));
    }

    @Test
    @DisplayName("Should throw exception for null source type")
    void testGetHandler_Null() {
        assertThrows(IllegalArgumentException.class, 
            () -> registry.getHandler(null));
    }

    @Test
    @DisplayName("Should throw exception for empty source type")
    void testGetHandler_Empty() {
        assertThrows(IllegalArgumentException.class, 
            () -> registry.getHandler(""));
    }

    @Test
    @DisplayName("Should return empty set when no handlers registered")
    void testGetRegisteredTypes_Empty() {
        assertTrue(registry.getRegisteredTypes().isEmpty());
    }
}
