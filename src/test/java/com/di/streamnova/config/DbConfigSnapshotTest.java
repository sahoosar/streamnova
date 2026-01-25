package com.di.streamnova.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test cases for DbConfigSnapshot record.
 */
@DisplayName("DbConfigSnapshot Tests")
class DbConfigSnapshotTest {

    @Test
    @DisplayName("Should create DbConfigSnapshot with all fields")
    void testCreateDbConfigSnapshot_AllFields() {
        DbConfigSnapshot config = new DbConfigSnapshot(
            "jdbc:postgresql://localhost:5432/mydb",
            "admin",
            "password123",
            "org.postgresql.Driver",
            10,
            5,
            30000L,
            5000L,
            1800000L
        );
        
        assertEquals("jdbc:postgresql://localhost:5432/mydb", config.jdbcUrl());
        assertEquals("admin", config.username());
        assertEquals("password123", config.password());
        assertEquals("org.postgresql.Driver", config.driverClassName());
        assertEquals(10, config.maximumPoolSize());
        assertEquals(5, config.minimumIdle());
        assertEquals(30000L, config.idleTimeoutMs());
        assertEquals(5000L, config.connectionTimeoutMs());
        assertEquals(1800000L, config.maxLifetimeMs());
    }

    @Test
    @DisplayName("Should be serializable")
    void testSerializable() {
        DbConfigSnapshot config = new DbConfigSnapshot(
            "jdbc:postgresql://localhost:5432/mydb",
            "admin",
            "password123",
            "org.postgresql.Driver",
            10,
            5,
            30000L,
            5000L,
            1800000L
        );
        
        // Verify it implements Serializable (records are serializable by default)
        assertTrue(config instanceof java.io.Serializable);
    }

    @Test
    @DisplayName("Should have correct toString representation")
    void testToString() {
        DbConfigSnapshot config = new DbConfigSnapshot(
            "jdbc:postgresql://localhost:5432/mydb",
            "admin",
            "password123",
            "org.postgresql.Driver",
            10,
            5,
            30000L,
            5000L,
            1800000L
        );
        
        String toString = config.toString();
        assertNotNull(toString);
        // Note: Java records include all fields in toString by default
        // In production, consider implementing a custom toString that masks passwords
        assertTrue(toString.contains("jdbc:postgresql"));
    }

    @Test
    @DisplayName("Should be equal when all fields match")
    void testEquals_SameValues() {
        DbConfigSnapshot config1 = new DbConfigSnapshot(
            "jdbc:postgresql://localhost:5432/mydb",
            "admin",
            "password123",
            "org.postgresql.Driver",
            10,
            5,
            30000L,
            5000L,
            1800000L
        );
        DbConfigSnapshot config2 = new DbConfigSnapshot(
            "jdbc:postgresql://localhost:5432/mydb",
            "admin",
            "password123",
            "org.postgresql.Driver",
            10,
            5,
            30000L,
            5000L,
            1800000L
        );
        
        assertEquals(config1, config2);
        assertEquals(config1.hashCode(), config2.hashCode());
    }

    @Test
    @DisplayName("Should not be equal when fields differ")
    void testEquals_DifferentValues() {
        DbConfigSnapshot config1 = new DbConfigSnapshot(
            "jdbc:postgresql://localhost:5432/mydb",
            "admin",
            "password123",
            "org.postgresql.Driver",
            10,
            5,
            30000L,
            5000L,
            1800000L
        );
        DbConfigSnapshot config2 = new DbConfigSnapshot(
            "jdbc:postgresql://localhost:5432/mydb",
            "admin2",
            "password123",
            "org.postgresql.Driver",
            10,
            5,
            30000L,
            5000L,
            1800000L
        );
        
        assertNotEquals(config1, config2);
    }
}
