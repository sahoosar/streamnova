package com.di.streamnova.util;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Date;
import java.sql.Time;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.List;

public class DateFormatUtils {
    private static final Logger logger = LoggerFactory.getLogger(DateFormatUtils.class);
    /**
     * Converts any date string to a numeric yyyyMMdd format.
     *
     * @param inputDate    the input date string (e.g. "06-02-2025")
     * @param inputPattern the pattern of the input date (e.g. "dd-MM-yyyy")
     * @return long formatted date (e.g. 20250206)
     */
    private static final List<String> KNOWN_PATTERNS = Arrays.asList(
            "yyyy-MM-dd",   // ISO standard
            "dd/MM/yyyy",   // UK / EU
            "MM-dd-yyyy",   // US
            "yyyy/MM/dd",   // Logs
            "dd-MM-yyyy",   // Forms
            "MM/dd/yyyy",   // US alternate
            "dd.MM.yyyy",   // Central Europe
            "yyyy.MM.dd" ,   // Asia / Legacy systems
            "yyyyMMdd"

    );

    /**
     * Tries known patterns to convert input date string to numeric yyyyMMdd format.
     *
     * @param inputDate the input date string (e.g. "21/06/2024")
     * @return formatted as long (e.g. 20240621)
     */
    public static long convertToNumericPartition(String inputDate) {
        for (String pattern : KNOWN_PATTERNS) {
            try {
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);
                LocalDate date = LocalDate.parse(inputDate, formatter);
                return Long.parseLong(date.format(DateTimeFormatter.BASIC_ISO_DATE));
            } catch (DateTimeParseException ignored) {
                logger.error("Pattern failed: {}, error: {}", pattern, ignored.getMessage());
            }
        }
        throw new IllegalArgumentException("Unrecognized date format: " + inputDate +
                ". Supported patterns are: " + String.join(", ", KNOWN_PATTERNS));
    }

    public static String convertToStringPartition(String inputDate) {
        for (String pattern : KNOWN_PATTERNS) {
            try {
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);
                LocalDate date = LocalDate.parse(inputDate, formatter);
                return date.format(DateTimeFormatter.BASIC_ISO_DATE);
            } catch (DateTimeParseException ignored) {
                logger.error("Pattern failed: {}, error: {}", pattern, ignored.getMessage());
            }
        }
        throw new IllegalArgumentException("Unrecognized date format: " + inputDate +
                ". Supported patterns are: " + String.join(", ", KNOWN_PATTERNS));
    }

    public static Date convertToDate(String inputDate) {
        for (String pattern : KNOWN_PATTERNS) {
            try {
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);
                //return LocalDate.parse(inputDate, formatter);
                return Date.valueOf(LocalDate.parse(inputDate, formatter));
            } catch (DateTimeParseException ignored) {}
        }
        throw new IllegalArgumentException("Unrecognized date format: " + inputDate);
    }

    /**
     * Converts SQL date/time objects to Joda Time DateTime for Apache Beam compatibility.
     * Apache Beam's DATETIME field type requires Joda Time DateTime objects.
     *
     * @param value the SQL date/time object (Timestamp, Date, Time, or java.util.Date)
     * @return Joda Time DateTime object, or null if input is null
     * @throws IllegalArgumentException if the value is not a recognized date/time type
     */
    public static DateTime convertToJodaDateTime(Object value) {
        if (value == null) {
            return null;
        }
        
        if (value instanceof java.sql.Timestamp) {
            return new DateTime(((java.sql.Timestamp) value).getTime());
        } else if (value instanceof Date) {
            return new DateTime(((Date) value).getTime());
        } else if (value instanceof Time) {
            // For Time, use current date with the time component
            long timeMillis = ((Time) value).getTime();
            DateTime today = new DateTime().withTimeAtStartOfDay();
            return today.plusMillis((int) timeMillis);
        } else if (value instanceof java.util.Date) {
            return new DateTime(((java.util.Date) value).getTime());
        }
        
        throw new IllegalArgumentException("Unsupported date/time type: " + value.getClass().getName());
    }
}
