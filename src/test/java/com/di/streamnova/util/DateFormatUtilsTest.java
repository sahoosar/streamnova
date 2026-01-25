package com.di.streamnova.util;

import org.joda.time.DateTime;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test cases for DateFormatUtils utility class.
 */
@DisplayName("DateFormatUtils Tests")
class DateFormatUtilsTest {

    // ============================================================================
    // convertToNumericPartition Tests
    // ============================================================================

    @ParameterizedTest
    @CsvSource({
        "2024-01-15, 20240115",
        "15/01/2024, 20240115",
        "01-15-2024, 20240115",
        "2024/01/15, 20240115",
        "15-01-2024, 20240115",
        "01/15/2024, 20240115",
        "15.01.2024, 20240115",
        "2024.01.15, 20240115",
        "20240115, 20240115"
    })
    @DisplayName("Should convert date strings to numeric format")
    void testConvertToNumericPartition_ValidFormats(String input, long expected) {
        long result = DateFormatUtils.convertToNumericPartition(input);
        assertEquals(expected, result);
    }

    @Test
    @DisplayName("Should throw exception for unrecognized date format")
    void testConvertToNumericPartition_InvalidFormat() {
        assertThrows(IllegalArgumentException.class, 
            () -> DateFormatUtils.convertToNumericPartition("invalid-date"));
    }

    // ============================================================================
    // convertToStringPartition Tests
    // ============================================================================

    @Test
    @DisplayName("Should convert date strings to string partition format")
    void testConvertToStringPartition_ValidFormats() {
        assertEquals("20240115", DateFormatUtils.convertToStringPartition("2024-01-15"));
        assertEquals("20240115", DateFormatUtils.convertToStringPartition("15/01/2024"));
    }

    @Test
    @DisplayName("Should throw exception for invalid string partition format")
    void testConvertToStringPartition_InvalidFormat() {
        assertThrows(IllegalArgumentException.class, 
            () -> DateFormatUtils.convertToStringPartition("invalid"));
    }

    // ============================================================================
    // convertToDate Tests
    // ============================================================================

    @Test
    @DisplayName("Should convert date strings to SQL Date")
    void testConvertToDate_ValidFormats() {
        Date result = DateFormatUtils.convertToDate("2024-01-15");
        assertNotNull(result);
        assertEquals(Date.valueOf("2024-01-15"), result);
    }

    @Test
    @DisplayName("Should throw exception for invalid date format")
    void testConvertToDate_InvalidFormat() {
        assertThrows(IllegalArgumentException.class, 
            () -> DateFormatUtils.convertToDate("invalid-date"));
    }

    // ============================================================================
    // convertToJodaDateTime Tests
    // ============================================================================

    @Test
    @DisplayName("Should convert SQL Timestamp to Joda DateTime")
    void testConvertToJodaDateTime_FromTimestamp() {
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        DateTime result = DateFormatUtils.convertToJodaDateTime(timestamp);
        assertNotNull(result);
        assertEquals(timestamp.getTime(), result.getMillis());
    }

    @Test
    @DisplayName("Should convert SQL Date to Joda DateTime")
    void testConvertToJodaDateTime_FromSqlDate() {
        Date sqlDate = Date.valueOf("2024-01-15");
        DateTime result = DateFormatUtils.convertToJodaDateTime(sqlDate);
        assertNotNull(result);
        assertEquals(sqlDate.getTime(), result.getMillis());
    }

    @Test
    @DisplayName("Should convert SQL Time to Joda DateTime")
    void testConvertToJodaDateTime_FromSqlTime() {
        Time sqlTime = Time.valueOf("12:30:45");
        DateTime result = DateFormatUtils.convertToJodaDateTime(sqlTime);
        assertNotNull(result);
        // Time conversion uses current date with time component
        assertTrue(result.getMillis() > 0);
    }

    @Test
    @DisplayName("Should convert java.util.Date to Joda DateTime")
    void testConvertToJodaDateTime_FromUtilDate() {
        java.util.Date utilDate = new java.util.Date(System.currentTimeMillis());
        DateTime result = DateFormatUtils.convertToJodaDateTime(utilDate);
        assertNotNull(result);
        assertEquals(utilDate.getTime(), result.getMillis());
    }

    @Test
    @DisplayName("Should return null for null input")
    void testConvertToJodaDateTime_Null() {
        assertNull(DateFormatUtils.convertToJodaDateTime(null));
    }

    @Test
    @DisplayName("Should throw exception for unsupported type")
    void testConvertToJodaDateTime_UnsupportedType() {
        assertThrows(IllegalArgumentException.class, 
            () -> DateFormatUtils.convertToJodaDateTime("not-a-date"));
    }
}
