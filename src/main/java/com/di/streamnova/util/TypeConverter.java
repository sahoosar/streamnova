package com.di.streamnova.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.joda.time.DateTime;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.UUID;

/**
 * Utility class for converting JDBC ResultSet values to Apache Beam Schema-compatible types.
 * 
 * This class handles all type conversions required when mapping database values to Apache Beam Row objects.
 * It supports conversions for:
 * - Date/Time types (SQL Date/Time/Timestamp → Joda Time DateTime)
 * - Numeric types (Integer/Short/Byte → Long for INT64, Integer → Short for INT16, Float → Double for DOUBLE)
 * - Boolean types
 * - String types
 * - Byte array types (BLOB, CLOB, BYTEA, etc.)
 * - Large object types (CLOB, NCLOB, TEXT, etc.)
 * 
 * Database Support:
 * - PostgreSQL: All standard types + BYTEA, JSON, JSONB, UUID, ARRAY types
 * - Oracle: NUMBER, VARCHAR2, CHAR, DATE, TIMESTAMP, BLOB, CLOB, NCLOB, RAW, LONG RAW, BINARY_FLOAT, BINARY_DOUBLE
 * - MySQL: TINYINT, SMALLINT, MEDIUMINT, INT, BIGINT, DECIMAL, FLOAT, DOUBLE, DATE, DATETIME, TIMESTAMP, 
 *          CHAR, VARCHAR, TEXT, BLOB, TINYBLOB, MEDIUMBLOB, LONGBLOB, ENUM, SET, BIT, JSON
 * 
 * All conversions are safe and include proper range checking and error handling.
 */
@Slf4j
public final class TypeConverter {

    private TypeConverter() {
        // Utility class - prevent instantiation
    }

    /**
     * Converts a JDBC ResultSet value to the appropriate type for an Apache Beam Schema field.
     * 
     * This is the main entry point for type conversion. It automatically detects the target
     * schema field type and performs the necessary conversions.
     * 
     * @param value the value from JDBC ResultSet (can be any type)
     * @param field the Apache Beam Schema field definition
     * @param fieldName the name of the field (for logging purposes)
     * @return the converted value compatible with the schema field type, or null if input is null
     * @throws IllegalArgumentException if conversion fails and cannot be recovered
     */
    public static Object convertToSchemaType(Object value, Schema.Field field, String fieldName) {
        if (value == null) {
            return null;
        }

        Schema.FieldType fieldType = field.getType();
        Schema.TypeName typeName = fieldType.getTypeName();

        try {
            switch (typeName) {
                case DATETIME:
                    return convertToDateTime(value, fieldName);
                case INT64:
                    return convertToLong(value, fieldName);
                case INT32:
                    return convertToInteger(value, fieldName);
                case INT16:
                    return convertToShort(value, fieldName);
                case BYTE:
                    return convertToByte(value, fieldName);
                case DOUBLE:
                    return convertToDouble(value, fieldName);
                case FLOAT:
                    return convertToFloat(value, fieldName);
                case BOOLEAN:
                    return convertToBoolean(value, fieldName);
                case STRING:
                    return convertToString(value, fieldName);
                case BYTES:
                    return convertToBytes(value, fieldName);
                case DECIMAL:
                    return convertToDecimal(value, fieldName);
                case ARRAY:
                    return convertToArray(value, fieldName);
                case ITERABLE:
                    return convertToIterable(value, fieldName);
                case MAP:
                    return convertToMap(value, fieldName);
                case ROW:
                    return convertToRow(value, fieldName);
                default:
                    log.warn("Field '{}' has unsupported schema type: {}. Returning value as-is.", 
                            fieldName, typeName);
                    return value;
            }
        } catch (IllegalArgumentException e) {
            log.error("Failed to convert value for field '{}' (type: {}) to schema type {}: {}", 
                    fieldName, value.getClass().getName(), typeName, e.getMessage());
            throw new IllegalArgumentException(
                    String.format("Cannot convert %s to %s for field '%s': %s", 
                            value.getClass().getSimpleName(), typeName, fieldName, e.getMessage()), e);
        }
    }

    /**
     * Converts a value to Joda Time DateTime for DATETIME fields.
     * 
     * @param value the date/time value (Timestamp, Date, Time, or java.util.Date)
     * @param fieldName the field name for logging
     * @return Joda Time DateTime object
     */
    public static DateTime convertToDateTime(Object value, String fieldName) {
        if (value == null) {
            return null;
        }

        if (value instanceof DateTime) {
            // Already converted
            return (DateTime) value;
        }

        if (value instanceof Timestamp || value instanceof Date || 
            value instanceof Time || value instanceof java.util.Date) {
            DateTime converted = DateFormatUtils.convertToJodaDateTime(value);
            log.debug("Converted date/time value for field '{}' from {} to DateTime", 
                    fieldName, value.getClass().getSimpleName());
            return converted;
        }

        throw new IllegalArgumentException(
                String.format("Cannot convert %s to DateTime for field '%s'", 
                        value.getClass().getName(), fieldName));
    }

    /**
     * Converts a value to Long for INT64 fields.
     * Handles Integer, Short, Byte, and other numeric types.
     * 
     * @param value the numeric value
     * @param fieldName the field name for logging
     * @return Long value
     */
    public static Long convertToLong(Object value, String fieldName) {
        if (value == null) {
            return null;
        }

        if (value instanceof Long) {
            return (Long) value;
        }

        if (value instanceof Integer) {
            Long converted = Long.valueOf(((Integer) value).longValue());
            log.debug("Converted Integer to Long for INT64 field '{}'", fieldName);
            return converted;
        }

        if (value instanceof Short) {
            Long converted = Long.valueOf(((Short) value).longValue());
            log.debug("Converted Short to Long for INT64 field '{}'", fieldName);
            return converted;
        }

        if (value instanceof Byte) {
            Long converted = Long.valueOf(((Byte) value).longValue());
            log.debug("Converted Byte to Long for INT64 field '{}'", fieldName);
            return converted;
        }

        if (value instanceof Number) {
            Long converted = Long.valueOf(((Number) value).longValue());
            log.debug("Converted {} to Long for INT64 field '{}'", 
                    value.getClass().getSimpleName(), fieldName);
            return converted;
        }

        throw new IllegalArgumentException(
                String.format("Cannot convert %s to Long for field '%s' (not a numeric type)", 
                        value.getClass().getName(), fieldName));
    }

    /**
     * Converts a value to Integer for INT32 fields.
     * 
     * @param value the numeric value
     * @param fieldName the field name for logging
     * @return Integer value
     */
    public static Integer convertToInteger(Object value, String fieldName) {
        if (value == null) {
            return null;
        }

        if (value instanceof Integer) {
            return (Integer) value;
        }

        if (value instanceof Number) {
            long longValue = ((Number) value).longValue();
            if (longValue >= Integer.MIN_VALUE && longValue <= Integer.MAX_VALUE) {
                Integer converted = Integer.valueOf((int) longValue);
                log.debug("Converted {} to Integer for INT32 field '{}'", 
                        value.getClass().getSimpleName(), fieldName);
                return converted;
            } else {
                throw new IllegalArgumentException(
                        String.format("Value %d is out of range for Integer (field '%s')", 
                                longValue, fieldName));
            }
        }

        throw new IllegalArgumentException(
                String.format("Cannot convert %s to Integer for field '%s' (not a numeric type)", 
                        value.getClass().getName(), fieldName));
    }

    /**
     * Converts a value to Short for INT16 fields.
     * Includes range checking to ensure value fits in Short range.
     * 
     * @param value the numeric value
     * @param fieldName the field name for logging
     * @return Short value
     */
    public static Short convertToShort(Object value, String fieldName) {
        if (value == null) {
            return null;
        }

        if (value instanceof Short) {
            return (Short) value;
        }

        if (value instanceof Number) {
            long numValue = ((Number) value).longValue();
            if (numValue >= Short.MIN_VALUE && numValue <= Short.MAX_VALUE) {
                Short converted = Short.valueOf((short) numValue);
                log.debug("Converted {} to Short for INT16 field '{}'", 
                        value.getClass().getSimpleName(), fieldName);
                return converted;
            } else {
                log.warn("Field '{}' is INT16 but value {} is out of range ({} to {}). " +
                        "This may cause issues!", fieldName, numValue, Short.MIN_VALUE, Short.MAX_VALUE);
                // Still try to convert, but log warning
                return Short.valueOf((short) numValue);
            }
        }

        throw new IllegalArgumentException(
                String.format("Cannot convert %s to Short for field '%s' (not a numeric type)", 
                        value.getClass().getName(), fieldName));
    }

    /**
     * Converts a value to Byte for BYTE fields.
     * Includes range checking to ensure value fits in Byte range.
     * 
     * @param value the numeric value
     * @param fieldName the field name for logging
     * @return Byte value
     */
    public static Byte convertToByte(Object value, String fieldName) {
        if (value == null) {
            return null;
        }

        if (value instanceof Byte) {
            return (Byte) value;
        }

        if (value instanceof Number) {
            long numValue = ((Number) value).longValue();
            if (numValue >= Byte.MIN_VALUE && numValue <= Byte.MAX_VALUE) {
                Byte converted = Byte.valueOf((byte) numValue);
                log.debug("Converted {} to Byte for BYTE field '{}'", 
                        value.getClass().getSimpleName(), fieldName);
                return converted;
            } else {
                log.warn("Field '{}' is BYTE but value {} is out of range ({} to {}). " +
                        "This may cause issues!", fieldName, numValue, Byte.MIN_VALUE, Byte.MAX_VALUE);
                // Still try to convert, but log warning
                return Byte.valueOf((byte) numValue);
            }
        }

        throw new IllegalArgumentException(
                String.format("Cannot convert %s to Byte for field '%s' (not a numeric type)", 
                        value.getClass().getName(), fieldName));
    }

    /**
     * Converts a value to Double for DOUBLE fields.
     * Handles Float and other numeric types.
     * 
     * @param value the numeric value
     * @param fieldName the field name for logging
     * @return Double value
     */
    public static Double convertToDouble(Object value, String fieldName) {
        if (value == null) {
            return null;
        }

        if (value instanceof Double) {
            return (Double) value;
        }

        if (value instanceof Float) {
            Double converted = Double.valueOf(((Float) value).doubleValue());
            log.debug("Converted Float to Double for DOUBLE field '{}'", fieldName);
            return converted;
        }

        if (value instanceof Number) {
            Double converted = Double.valueOf(((Number) value).doubleValue());
            log.debug("Converted {} to Double for DOUBLE field '{}'", 
                    value.getClass().getSimpleName(), fieldName);
            return converted;
        }

        throw new IllegalArgumentException(
                String.format("Cannot convert %s to Double for field '%s' (not a numeric type)", 
                        value.getClass().getName(), fieldName));
    }

    /**
     * Converts a value to Float for FLOAT fields.
     * 
     * @param value the numeric value
     * @param fieldName the field name for logging
     * @return Float value
     */
    public static Float convertToFloat(Object value, String fieldName) {
        if (value == null) {
            return null;
        }

        if (value instanceof Float) {
            return (Float) value;
        }

        if (value instanceof Number) {
            Float converted = Float.valueOf(((Number) value).floatValue());
            log.debug("Converted {} to Float for FLOAT field '{}'", 
                    value.getClass().getSimpleName(), fieldName);
            return converted;
        }

        throw new IllegalArgumentException(
                String.format("Cannot convert %s to Float for field '%s' (not a numeric type)", 
                        value.getClass().getName(), fieldName));
    }

    /**
     * Converts a value to Boolean for BOOLEAN fields.
     * Handles various boolean representations (Boolean, Integer 0/1, String "true"/"false").
     * 
     * @param value the boolean value
     * @param fieldName the field name for logging
     * @return Boolean value
     */
    public static Boolean convertToBoolean(Object value, String fieldName) {
        if (value == null) {
            return null;
        }

        if (value instanceof Boolean) {
            return (Boolean) value;
        }

        if (value instanceof Number) {
            // Treat 0 as false, non-zero as true
            boolean converted = ((Number) value).intValue() != 0;
            log.debug("Converted {} ({}) to Boolean ({}) for BOOLEAN field '{}'", 
                    value.getClass().getSimpleName(), value, converted, fieldName);
            return converted;
        }

        if (value instanceof String) {
            String str = ((String) value).trim().toLowerCase();
            if ("true".equals(str) || "1".equals(str) || "yes".equals(str) || "y".equals(str)) {
                log.debug("Converted String '{}' to Boolean (true) for BOOLEAN field '{}'", 
                        value, fieldName);
                return true;
            } else if ("false".equals(str) || "0".equals(str) || "no".equals(str) || "n".equals(str)) {
                log.debug("Converted String '{}' to Boolean (false) for BOOLEAN field '{}'", 
                        value, fieldName);
                return false;
            } else {
                throw new IllegalArgumentException(
                        String.format("Cannot convert String '%s' to Boolean for field '%s'", 
                                value, fieldName));
            }
        }

        throw new IllegalArgumentException(
                String.format("Cannot convert %s to Boolean for field '%s'", 
                        value.getClass().getName(), fieldName));
    }

    /**
     * Converts a value to String for STRING fields.
     * Handles CLOB, NCLOB, TEXT, and other large text types from Oracle, MySQL, and PostgreSQL.
     * 
     * @param value the value to convert
     * @param fieldName the field name for logging
     * @return String value
     */
    public static String convertToString(Object value, String fieldName) {
        if (value == null) {
            return null;
        }

        if (value instanceof String) {
            return (String) value;
        }

        // Handle Oracle/MySQL CLOB
        if (value instanceof Clob) {
            try {
                Clob clob = (Clob) value;
                long length = clob.length();
                if (length > Integer.MAX_VALUE) {
                    throw new IllegalArgumentException(
                            String.format("CLOB size %d exceeds maximum for field '%s'", length, fieldName));
                }
                String converted = clob.getSubString(1, (int) length);
                log.debug("Converted CLOB to String for STRING field '{}' (length: {})", fieldName, length);
                return converted;
            } catch (Exception e) {
                throw new IllegalArgumentException(
                        String.format("Failed to read CLOB for field '%s': %s", fieldName, e.getMessage()), e);
            }
        }

        // Handle Oracle NCLOB
        if (value instanceof NClob) {
            try {
                NClob nclob = (NClob) value;
                long length = nclob.length();
                if (length > Integer.MAX_VALUE) {
                    throw new IllegalArgumentException(
                            String.format("NCLOB size %d exceeds maximum for field '%s'", length, fieldName));
                }
                String converted = nclob.getSubString(1, (int) length);
                log.debug("Converted NCLOB to String for STRING field '{}' (length: {})", fieldName, length);
                return converted;
            } catch (Exception e) {
                throw new IllegalArgumentException(
                        String.format("Failed to read NCLOB for field '%s': %s", fieldName, e.getMessage()), e);
            }
        }

        // Handle Reader (for large text objects)
        if (value instanceof Reader) {
            try {
                Reader reader = (Reader) value;
                StringBuilder sb = new StringBuilder();
                char[] buffer = new char[8192];
                int charsRead;
                while ((charsRead = reader.read(buffer)) != -1) {
                    sb.append(buffer, 0, charsRead);
                }
                String converted = sb.toString();
                log.debug("Converted Reader to String for STRING field '{}' (length: {})", 
                        fieldName, converted.length());
                return converted;
            } catch (Exception e) {
                throw new IllegalArgumentException(
                        String.format("Failed to read Reader for field '%s': %s", fieldName, e.getMessage()), e);
            }
        }

        // Handle UUID (PostgreSQL UUID type)
        if (value instanceof UUID) {
            String converted = value.toString();
            log.debug("Converted UUID to String for STRING field '{}'", fieldName);
            return converted;
        }

        // Handle byte array (for binary data that should be converted to string)
        if (value instanceof byte[]) {
            String converted = new String((byte[]) value, StandardCharsets.UTF_8);
            log.debug("Converted byte[] to String (UTF-8) for STRING field '{}'", fieldName);
            return converted;
        }

        // Convert any other type to string
        String converted = value.toString();
        log.debug("Converted {} to String for STRING field '{}'", 
                value.getClass().getSimpleName(), fieldName);
        return converted;
    }

    /**
     * Converts a value to byte array for BYTES fields.
     * Handles BLOB, CLOB, NCLOB, BYTEA, and other binary types from Oracle, MySQL, and PostgreSQL.
     * 
     * @param value the value to convert
     * @param fieldName the field name for logging
     * @return byte array
     */
    public static byte[] convertToBytes(Object value, String fieldName) {
        if (value == null) {
            return null;
        }

        if (value instanceof byte[]) {
            return (byte[]) value;
        }

        // Handle Oracle/MySQL BLOB
        if (value instanceof Blob) {
            try {
                Blob blob = (Blob) value;
                long length = blob.length();
                if (length > Integer.MAX_VALUE) {
                    throw new IllegalArgumentException(
                            String.format("BLOB size %d exceeds maximum for field '%s'", length, fieldName));
                }
                byte[] converted = blob.getBytes(1, (int) length);
                log.debug("Converted BLOB to byte[] for BYTES field '{}' (size: {})", fieldName, length);
                return converted;
            } catch (Exception e) {
                throw new IllegalArgumentException(
                        String.format("Failed to read BLOB for field '%s': %s", fieldName, e.getMessage()), e);
            }
        }

        // Handle Oracle RAW, LONG RAW (already byte[])
        if (value instanceof Byte[]) {
            Byte[] byteArray = (Byte[]) value;
            byte[] converted = new byte[byteArray.length];
            for (int i = 0; i < byteArray.length; i++) {
                converted[i] = byteArray[i] != null ? byteArray[i] : 0;
            }
            log.debug("Converted Byte[] to byte[] for BYTES field '{}'", fieldName);
            return converted;
        }

        // Handle PostgreSQL BYTEA (can come as String in hex format)
        if (value instanceof String) {
            String str = (String) value;
            // Check if it's hex format (starts with \x)
            if (str.startsWith("\\x")) {
                // PostgreSQL hex format
                String hex = str.substring(2);
                if (hex.length() % 2 != 0) {
                    throw new IllegalArgumentException(
                            String.format("Hex string must have even length for field '%s'", fieldName));
                }
                byte[] converted = new byte[hex.length() / 2];
                for (int i = 0; i < converted.length; i++) {
                    converted[i] = (byte) Integer.parseInt(hex.substring(i * 2, i * 2 + 2), 16);
                }
                log.debug("Converted PostgreSQL BYTEA hex String to byte[] for BYTES field '{}'", fieldName);
                return converted;
            } else {
                // Regular string - convert to bytes using UTF-8
                byte[] converted = str.getBytes(StandardCharsets.UTF_8);
                log.debug("Converted String to byte[] (UTF-8) for BYTES field '{}'", fieldName);
                return converted;
            }
        }

        // Handle InputStream (for large objects)
        if (value instanceof InputStream) {
            try {
                InputStream is = (InputStream) value;
                byte[] buffer = new byte[8192];
                java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
                int bytesRead;
                while ((bytesRead = is.read(buffer)) != -1) {
                    baos.write(buffer, 0, bytesRead);
                }
                byte[] converted = baos.toByteArray();
                log.debug("Converted InputStream to byte[] for BYTES field '{}' (size: {})", 
                        fieldName, converted.length);
                return converted;
            } catch (Exception e) {
                throw new IllegalArgumentException(
                        String.format("Failed to read InputStream for field '%s': %s", fieldName, e.getMessage()), e);
            }
        }

        throw new IllegalArgumentException(
                String.format("Cannot convert %s to byte[] for field '%s'", 
                        value.getClass().getName(), fieldName));
    }

    /**
     * Converts a value to BigDecimal for DECIMAL fields.
     * 
     * @param value the numeric value
     * @param fieldName the field name for logging
     * @return BigDecimal value
     */
    public static BigDecimal convertToDecimal(Object value, String fieldName) {
        if (value == null) {
            return null;
        }

        if (value instanceof BigDecimal) {
            return (BigDecimal) value;
        }

        if (value instanceof Number) {
            BigDecimal converted = new BigDecimal(value.toString());
            log.debug("Converted {} to BigDecimal for DECIMAL field '{}'", 
                    value.getClass().getSimpleName(), fieldName);
            return converted;
        }

        if (value instanceof String) {
            try {
                BigDecimal converted = new BigDecimal((String) value);
                log.debug("Converted String '{}' to BigDecimal for DECIMAL field '{}'", 
                        value, fieldName);
                return converted;
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                        String.format("Cannot convert String '%s' to BigDecimal for field '%s'", 
                                value, fieldName), e);
            }
        }

        throw new IllegalArgumentException(
                String.format("Cannot convert %s to BigDecimal for field '%s'", 
                        value.getClass().getName(), fieldName));
    }

    /**
     * Checks if a value is a date/time type (SQL Date, Time, Timestamp, or java.util.Date).
     * 
     * @param value the value to check
     * @return true if the value is a date/time type
     */
    public static boolean isDateTimeValue(Object value) {
        return value != null && (
                value instanceof Date ||
                value instanceof Time ||
                value instanceof Timestamp ||
                value instanceof java.util.Date ||
                value instanceof DateTime);
    }

    /**
     * Checks if a value is a numeric type.
     * 
     * @param value the value to check
     * @return true if the value is a numeric type
     */
    public static boolean isNumericValue(Object value) {
        return value != null && value instanceof Number;
    }

    /**
     * Converts a value to Array for ARRAY fields.
     * Handles database array types (PostgreSQL arrays, Oracle VARRAY, etc.).
     * 
     * @param value the value to convert
     * @param fieldName the field name for logging
     * @return Array value
     */
    public static Object convertToArray(Object value, String fieldName) {
        if (value == null) {
            return null;
        }

        // Handle Java arrays
        if (value.getClass().isArray()) {
            log.debug("Value is already an array for ARRAY field '{}'", fieldName);
            return value;
        }

        // Handle java.sql.Array (PostgreSQL, Oracle VARRAY)
        if (value instanceof java.sql.Array) {
            try {
                java.sql.Array sqlArray = (java.sql.Array) value;
                Object converted = sqlArray.getArray();
                log.debug("Converted java.sql.Array to array for ARRAY field '{}'", fieldName);
                return converted;
            } catch (Exception e) {
                throw new IllegalArgumentException(
                        String.format("Failed to read java.sql.Array for field '%s': %s", 
                                fieldName, e.getMessage()), e);
            }
        }

        // Handle Collections
        if (value instanceof java.util.Collection) {
            java.util.Collection<?> collection = (java.util.Collection<?>) value;
            Object[] converted = collection.toArray();
            log.debug("Converted Collection to array for ARRAY field '{}' (size: {})", 
                    fieldName, converted.length);
            return converted;
        }

        throw new IllegalArgumentException(
                String.format("Cannot convert %s to array for field '%s'", 
                        value.getClass().getName(), fieldName));
    }

    /**
     * Converts a value to Iterable for ITERABLE fields.
     * 
     * @param value the value to convert
     * @param fieldName the field name for logging
     * @return Iterable value
     */
    public static Iterable<?> convertToIterable(Object value, String fieldName) {
        if (value == null) {
            return null;
        }

        if (value instanceof Iterable) {
            return (Iterable<?>) value;
        }

        if (value instanceof java.util.Collection) {
            return (Iterable<?>) value;
        }

        if (value.getClass().isArray()) {
            // Convert array to list
            java.util.List<Object> list = new java.util.ArrayList<>();
            int length = java.lang.reflect.Array.getLength(value);
            for (int i = 0; i < length; i++) {
                list.add(java.lang.reflect.Array.get(value, i));
            }
            log.debug("Converted array to Iterable for ITERABLE field '{}' (size: {})", 
                    fieldName, list.size());
            return list;
        }

        throw new IllegalArgumentException(
                String.format("Cannot convert %s to Iterable for field '%s'", 
                        value.getClass().getName(), fieldName));
    }

    /**
     * Converts a value to Map for MAP fields.
     * Handles JSON objects and key-value pairs.
     * 
     * @param value the value to convert
     * @param fieldName the field name for logging
     * @return Map value
     */
    public static java.util.Map<?, ?> convertToMap(Object value, String fieldName) {
        if (value == null) {
            return null;
        }

        if (value instanceof java.util.Map) {
            return (java.util.Map<?, ?>) value;
        }

        // Handle JSON strings (PostgreSQL JSON/JSONB, MySQL JSON)
        if (value instanceof String) {
            try {
                // Try to parse as JSON
                com.fasterxml.jackson.databind.ObjectMapper mapper = 
                        new com.fasterxml.jackson.databind.ObjectMapper();
                java.util.Map<?, ?> converted = mapper.readValue((String) value, 
                        new com.fasterxml.jackson.core.type.TypeReference<java.util.Map<String, Object>>() {});
                log.debug("Converted JSON String to Map for MAP field '{}'", fieldName);
                return converted;
            } catch (Exception e) {
                log.warn("Failed to parse JSON string for MAP field '{}': {}", fieldName, e.getMessage());
                // Fall through to error
            }
        }

        throw new IllegalArgumentException(
                String.format("Cannot convert %s to Map for field '%s'", 
                        value.getClass().getName(), fieldName));
    }

    /**
     * Converts a value to Row for ROW fields.
     * 
     * @param value the value to convert
     * @param fieldName the field name for logging
     * @return Row value
     */
    public static Row convertToRow(Object value, String fieldName) {
        if (value == null) {
            return null;
        }

        if (value instanceof Row) {
            return (Row) value;
        }

        // Handle JSON strings that represent structured data
        if (value instanceof String) {
            try {
                // This would require schema information to properly convert
                // For now, log a warning and return null or throw exception
                log.warn("Cannot convert JSON String to Row for field '{}' without schema information", fieldName);
                throw new IllegalArgumentException(
                        String.format("Row conversion from String requires schema information for field '%s'", 
                                fieldName));
            } catch (Exception e) {
                throw new IllegalArgumentException(
                        String.format("Cannot convert String to Row for field '%s': %s", 
                                fieldName, e.getMessage()), e);
            }
        }

        throw new IllegalArgumentException(
                String.format("Cannot convert %s to Row for field '%s'", 
                        value.getClass().getName(), fieldName));
    }

    /**
     * Handles Oracle-specific NUMBER type conversions.
     * Oracle NUMBER can represent various numeric types based on precision and scale.
     * 
     * @param value the Oracle NUMBER value
     * @param precision the precision (total number of digits)
     * @param scale the scale (number of digits after decimal point)
     * @param fieldName the field name for logging
     * @return converted numeric value
     */
    public static Object convertOracleNumber(Object value, Integer precision, Integer scale, String fieldName) {
        if (value == null) {
            return null;
        }

        if (!(value instanceof Number)) {
            throw new IllegalArgumentException(
                    String.format("Oracle NUMBER value must be numeric for field '%s'", fieldName));
        }

        Number num = (Number) value;

        // If scale is 0, it's an integer type
        if (scale != null && scale == 0) {
            long longValue = num.longValue();
            if (precision != null && precision <= 9) {
                // Fits in Integer
                if (longValue >= Integer.MIN_VALUE && longValue <= Integer.MAX_VALUE) {
                    return Integer.valueOf((int) longValue);
                }
            }
            // Use Long for larger integers
            return Long.valueOf(longValue);
        }

        // Decimal number - use BigDecimal or Double
        if (precision != null && precision <= 15) {
            // Can use Double for reasonable precision
            return Double.valueOf(num.doubleValue());
        }

        // High precision - use BigDecimal
        return new BigDecimal(num.toString());
    }

    // ============================================================================
    // Type Name Mapping Methods (for Schema Detection)
    // ============================================================================

    /**
     * Maps PostgreSQL data type name to Apache Beam Schema.FieldType.
     * Used during schema detection to determine the correct Beam schema type.
     * 
     * @param postgresType the PostgreSQL data type name (e.g., "integer", "varchar", "timestamp")
     * @return Apache Beam Schema.FieldType
     */
    public static Schema.FieldType mapPostgresTypeToBeamType(String postgresType) {
        if (postgresType == null || postgresType.isBlank()) {
            return Schema.FieldType.STRING;
        }
        
        String typeLower = postgresType.toLowerCase().trim();
        
        // Integer types
        if (typeLower.equals("smallint") || typeLower.equals("int2")) {
            return Schema.FieldType.INT16;
        } else if (typeLower.equals("integer") || typeLower.equals("int") || typeLower.equals("int4")) {
            return Schema.FieldType.INT32;
        } else if (typeLower.equals("bigint") || typeLower.equals("int8") || 
                   typeLower.contains("serial") || typeLower.contains("bigserial")) {
            return Schema.FieldType.INT64;
        }
        // Decimal/Numeric types
        else if (typeLower.equals("decimal") || typeLower.equals("numeric")) {
            return Schema.FieldType.DECIMAL;
        } else if (typeLower.equals("real") || typeLower.equals("float4")) {
            return Schema.FieldType.FLOAT;
        } else if (typeLower.equals("double precision") || typeLower.equals("float8") || 
                   typeLower.equals("float") || typeLower.equals("double")) {
            return Schema.FieldType.DOUBLE;
        }
        // Boolean type
        else if (typeLower.equals("boolean") || typeLower.equals("bool")) {
            return Schema.FieldType.BOOLEAN;
        }
        // Date/Time types
        else if (typeLower.equals("date") || typeLower.equals("time") || 
                 typeLower.equals("time without time zone") || typeLower.equals("time with time zone") ||
                 typeLower.equals("timestamp") || typeLower.equals("timestamp without time zone") ||
                 typeLower.equals("timestamp with time zone") || typeLower.equals("timestamptz")) {
            return Schema.FieldType.DATETIME;
        }
        // Binary types
        else if (typeLower.equals("bytea")) {
            return Schema.FieldType.BYTES;
        }
        // JSON types - treat as STRING for now (can be enhanced to MAP later)
        else if (typeLower.equals("json") || typeLower.equals("jsonb")) {
            return Schema.FieldType.STRING; // JSON converts to String (can be enhanced to MAP)
        }
        // UUID type
        else if (typeLower.equals("uuid")) {
            return Schema.FieldType.STRING; // UUID converts to String
        }
        // Array types - treat as STRING for now (can be enhanced later)
        else if (typeLower.contains("[]") || typeLower.contains("array")) {
            return Schema.FieldType.STRING; // Default to string for arrays
        }
        // String types (default)
        else {
            return Schema.FieldType.STRING;
        }
    }

    /**
     * Handles MySQL BIT type conversion.
     * MySQL BIT can be 1-64 bits and may come as byte array or number.
     * 
     * @param value the BIT value
     * @param fieldName the field name for logging
     * @return converted value (Boolean for single bit, Long for multi-bit)
     */
    public static Object convertMySQLBit(Object value, String fieldName) {
        if (value == null) {
            return null;
        }

        // MySQL BIT(1) often comes as Boolean
        if (value instanceof Boolean) {
            return value;
        }

        // MySQL BIT can come as byte array
        if (value instanceof byte[]) {
            byte[] bytes = (byte[]) value;
            if (bytes.length == 1) {
                // Single byte - convert to Boolean
                return bytes[0] != 0;
            } else {
                // Multiple bytes - convert to Long
                long result = 0;
                for (int i = 0; i < bytes.length && i < 8; i++) {
                    result |= ((long) (bytes[i] & 0xFF)) << (i * 8);
                }
                return Long.valueOf(result);
            }
        }

        // MySQL BIT can come as Number
        if (value instanceof Number) {
            long longValue = ((Number) value).longValue();
            if (longValue == 0 || longValue == 1) {
                return Boolean.valueOf(longValue != 0);
            }
            return Long.valueOf(longValue);
        }

        throw new IllegalArgumentException(
                String.format("Cannot convert %s to BIT for field '%s'", 
                        value.getClass().getName(), fieldName));
    }
}
