package com.di.streamnova.util;

import lombok.extern.slf4j.Slf4j;

import java.io.PrintStream;

/**
 * A PrintStream that filters out standalone "null" lines to prevent them from appearing in console output.
 * This is useful when third-party libraries (like Apache Beam) print null exception messages directly to stdout/stderr.
 * 
 * <p><strong>Important:</strong> This class only filters output to console, it does NOT affect:
 * <ul>
 *   <li>SLF4J/Logback logging (uses its own appenders)</li>
 *   <li>Exception handling or error propagation</li>
 *   <li>Method return values</li>
 *   <li>Any other application logic</li>
 * </ul>
 * 
 * <p><strong>Production-Ready Features:</strong>
 * <ul>
 *   <li>✅ Thread-safe: Uses ThreadLocal for per-thread state tracking</li>
 *   <li>✅ Only filters standalone "null" lines (not nulls in stack traces)</li>
 *   <li>✅ Logs filtered messages at DEBUG level for tracking (if enabled)</li>
 *   <li>✅ Preserves all stack trace information</li>
 *   <li>✅ Configurable via system properties</li>
 *   <li>✅ Minimal performance overhead</li>
 *   <li>✅ Safe to use in multi-threaded environments</li>
 *   <li>✅ Comprehensive: Overrides all println/print methods for consistency</li>
 * </ul>
 * 
 * <p><strong>Configuration:</strong>
 * <ul>
 *   <li>{@code filter.null.output=true} - Enable/disable filtering (default: true)</li>
 *   <li>{@code log.filtered.null.messages=true} - Log filtered messages at DEBUG level (default: true)</li>
 * </ul>
 * 
 * <p>This is purely a cosmetic filter for console output to reduce noise from third-party libraries.
 * All application logging via SLF4J/Logback is unaffected.
 */
@Slf4j
public class NullFilteringPrintStream extends PrintStream {
    private final PrintStream original;
    private final boolean filterNull;
    private final boolean logFilteredMessages;
    
    /**
     * Thread-local state to track if we're in the middle of a stack trace.
     * This ensures thread-safety in multi-threaded environments.
     */
    private static final ThreadLocal<Boolean> inStackTrace = ThreadLocal.withInitial(() -> false);

    /**
     * Creates a new NullFilteringPrintStream.
     * 
     * @param original the original PrintStream to delegate to
     * @param filterNull if true, filters out null values and "null" strings
     */
    public NullFilteringPrintStream(PrintStream original, boolean filterNull) {
        this(original, filterNull, shouldLogFilteredMessages());
    }

    /**
     * Creates a new NullFilteringPrintStream.
     * 
     * @param original the original PrintStream to delegate to
     * @param filterNull if true, filters out null values and "null" strings
     * @param logFilteredMessages if true, logs filtered messages at DEBUG level
     */
    public NullFilteringPrintStream(PrintStream original, boolean filterNull, boolean logFilteredMessages) {
        super(original);
        this.original = original;
        this.filterNull = filterNull;
        this.logFilteredMessages = logFilteredMessages;
    }

    /**
     * Checks if filtered messages should be logged based on system property.
     */
    private static boolean shouldLogFilteredMessages() {
        return Boolean.parseBoolean(System.getProperty("log.filtered.null.messages", "true"));
    }

    // ========== String Methods ==========
    
    @Override
    public void println(String x) {
        if (filterNull && shouldFilter(x)) {
            logFilteredMessage("Filtered null line from console output: " + x);
            return;
        }
        updateStackTraceState(x);
        original.println(x);
    }

    @Override
    public void print(String s) {
        if (filterNull && shouldFilter(s)) {
            logFilteredMessage("Filtered null string from console output: " + s);
            return;
        }
        updateStackTraceState(s);
        original.print(s);
    }

    // ========== Object Methods ==========
    
    @Override
    public void println(Object x) {
        if (filterNull && x == null && !inStackTrace.get()) {
            logFilteredMessage("Filtered null object from console output");
            return;
        }
        if (x != null) {
            updateStackTraceState(x.toString());
        }
        original.println(x);
    }

    @Override
    public void print(Object obj) {
        if (filterNull && obj == null && !inStackTrace.get()) {
            logFilteredMessage("Filtered null object from console output");
            return;
        }
        if (obj != null) {
            updateStackTraceState(obj.toString());
        }
        original.print(obj);
    }

    // ========== Primitive Methods (delegated - no filtering needed) ==========
    // These methods never print "null" - they print actual values, so we just delegate
    
    @Override
    public void println() {
        original.println();
    }

    @Override
    public void println(boolean x) {
        original.println(x);
    }

    @Override
    public void println(char x) {
        original.println(x);
    }

    @Override
    public void println(int x) {
        original.println(x);
    }

    @Override
    public void println(long x) {
        original.println(x);
    }

    @Override
    public void println(float x) {
        original.println(x);
    }

    @Override
    public void println(double x) {
        original.println(x);
    }

    @Override
    public void print(boolean b) {
        original.print(b);
    }

    @Override
    public void print(char c) {
        original.print(c);
    }

    @Override
    public void print(int i) {
        original.print(i);
    }

    @Override
    public void print(long l) {
        original.print(l);
    }

    @Override
    public void print(float f) {
        original.print(f);
    }

    @Override
    public void print(double d) {
        original.print(d);
    }

    // ========== Array Methods ==========
    
    @Override
    public void println(char[] x) {
        // char[] can be null, but when printed it shows as actual characters or "null"
        if (filterNull && x == null && !inStackTrace.get()) {
            logFilteredMessage("Filtered null char array from console output");
            return;
        }
        if (x != null) {
            updateStackTraceState(new String(x));
        }
        original.println(x);
    }

    @Override
    public void print(char[] s) {
        // char[] can be null, but when printed it shows as actual characters or "null"
        if (filterNull && s == null && !inStackTrace.get()) {
            logFilteredMessage("Filtered null char array from console output");
            return;
        }
        if (s != null) {
            updateStackTraceState(new String(s));
        }
        original.print(s);
    }

    // ========== Helper Methods ==========

    /**
     * Updates the stack trace state based on the content.
     * Stack traces contain "at" and "Caused by" which we should preserve.
     * This method is thread-safe as it uses ThreadLocal.
     */
    private void updateStackTraceState(String content) {
        if (content == null) {
            return;
        }
        
        String trimmed = content.trim();
        boolean currentlyInStackTrace = inStackTrace.get();
        
        // Detect stack trace patterns
        if (trimmed.startsWith("at ") || 
            trimmed.startsWith("Caused by:") ||
            trimmed.startsWith("Exception in thread") ||
            trimmed.contains("Exception:") ||
            trimmed.contains("Error:") ||
            trimmed.matches("^\\s+at\\s+.*")) {
            inStackTrace.set(true);
        } else if (trimmed.isEmpty() && currentlyInStackTrace) {
            // Empty line after stack trace - might be end of trace
            // Keep inStackTrace true for a bit longer
        } else if (!trimmed.isEmpty() && 
                   !trimmed.startsWith("\t") && 
                   !trimmed.startsWith("   ") &&
                   !trimmed.matches("^\\s+.*")) {
            // Non-indented line that's not stack trace related - likely end of trace
            inStackTrace.set(false);
        }
    }

    /**
     * Determines if a string should be filtered out.
     * Only filters standalone "null" lines, not nulls that are part of stack traces.
     * This method is thread-safe as it uses ThreadLocal.
     * 
     * @param s the string to check
     * @return true if the string should be filtered (null or equals "null")
     */
    private boolean shouldFilter(String s) {
        if (inStackTrace.get()) {
            // Never filter nulls in stack traces - they might be important
            return false;
        }
        return s == null || s.trim().equals("null");
    }

    /**
     * Logs a filtered message at DEBUG level if logging is enabled.
     * This ensures filtered messages are not completely lost and can be tracked.
     */
    private void logFilteredMessage(String message) {
        if (logFilteredMessages) {
            log.debug("[NullFilter] {}", message);
        }
    }

    // ========== Stream Management Methods ==========
    
    @Override
    public void flush() {
        original.flush();
    }

    @Override
    public void close() {
        // Don't close the original stream - it's System.out/System.err
        // Closing System.out/System.err would break console output for the entire JVM
        // Also clean up ThreadLocal to prevent memory leaks
        inStackTrace.remove();
    }

    @Override
    public boolean checkError() {
        return original.checkError();
    }
    
    // Note: setError() and clearError() are protected methods in PrintStream
    // and cannot be overridden from a subclass. They are handled internally by PrintStream.

    @Override
    public PrintStream append(CharSequence csq) {
        if (csq != null) {
            updateStackTraceState(csq.toString());
        }
        return original.append(csq);
    }

    @Override
    public PrintStream append(CharSequence csq, int start, int end) {
        if (csq != null) {
            updateStackTraceState(csq.subSequence(start, end).toString());
        }
        return original.append(csq, start, end);
    }

    @Override
    public PrintStream append(char c) {
        return original.append(c);
    }

    @Override
    public void write(int b) {
        original.write(b);
    }

    @Override
    public void write(byte[] buf, int off, int len) {
        original.write(buf, off, len);
    }
}
