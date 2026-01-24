# Logback Color Configuration

## ✅ Color Support Enabled

The logback configuration now includes color support for different log levels using the `%highlight` converter.

## Current Configuration

**File:** `src/main/resources/logback-spring.xml`

```xml
<pattern>%d{HH:mm:ss.SSS} %highlight(%-5level) [%t] [app=%property{appName}] %logger{36} - %msg%n</pattern>
```

## Default Colors (Automatic)

The `%highlight` converter automatically applies these colors:

| Level | Color | Example |
|-------|-------|---------|
| **ERROR** | 🔴 Red | `ERROR` |
| **WARN** | 🟡 Yellow | `WARN` |
| **INFO** | 🟢 Green | `INFO` |
| **DEBUG** | 🔵 Cyan | `DEBUG` |
| **TRACE** | ⚪ White | `TRACE` |

## Custom Color Configuration (Optional)

If you want to customize the colors, you can use the `%clr` converter with explicit color definitions:

### Option 1: Custom Colors with %clr

```xml
<pattern>%d{HH:mm:ss.SSS} %clr(%-5level){red=ERROR, yellow=WARN, green=INFO, cyan=DEBUG, white=TRACE} [%t] [app=%property{appName}] %logger{36} - %msg%n</pattern>
```

### Option 2: Color Entire Message

```xml
<pattern>%d{HH:mm:ss.SSS} %clr(%-5level){red=ERROR, yellow=WARN, green=INFO, cyan=DEBUG} %clr([%t]){blue} [app=%property{appName}] %logger{36} - %msg%n</pattern>
```

### Option 3: Color by Level with Custom Colors

```xml
<pattern>%d{HH:mm:ss.SSS} %highlight(%-5level){ERROR=red blink, WARN=yellow bold, INFO=green, DEBUG=cyan} [%t] [app=%property{appName}] %logger{36} - %msg%n</pattern>
```

## Available Color Options

### Basic Colors
- `black`
- `red`
- `green`
- `yellow`
- `blue`
- `magenta`
- `cyan`
- `white`
- `gray`
- `brightred`
- `brightgreen`
- `brightyellow`
- `brightblue`
- `brightmagenta`
- `brightcyan`
- `brightwhite`

### Style Options
- `bold` - Bold text
- `faint` - Dim text
- `italic` - Italic text
- `underline` - Underlined text
- `blink` - Blinking text (not supported in all terminals)

### Combined Styles
```xml
%highlight(%-5level){ERROR=red bold, WARN=yellow bold, INFO=green, DEBUG=cyan}
```

## Terminal Compatibility

### ✅ Supported Terminals
- **Linux/Unix terminals** (xterm, gnome-terminal, etc.)
- **macOS Terminal** and **iTerm2**
- **Windows Terminal** (Windows 10/11)
- **VS Code Integrated Terminal**
- **IntelliJ IDEA Terminal**
- **Eclipse Console**

### ⚠️ Not Supported
- **Plain text files** (colors are ANSI escape codes)
- **Some older terminals** (may show escape codes as text)

## Disabling Colors

If you want to disable colors (e.g., when redirecting to a file), you can:

### Option 1: Use System Property

```bash
# Disable colors
java -Dspring.output.ansi.enabled=never -jar app.jar

# Enable colors (default)
java -Dspring.output.ansi.enabled=always -jar app.jar

# Auto-detect (default)
java -Dspring.output.ansi.enabled=detect -jar app.jar
```

### Option 2: Conditional Pattern

```xml
<if condition='property("spring.output.ansi.enabled").equals("never")'>
    <then>
        <pattern>%d{HH:mm:ss.SSS} %-5level [%t] [app=%property{appName}] %logger{36} - %msg%n</pattern>
    </then>
    <else>
        <pattern>%d{HH:mm:ss.SSS} %highlight(%-5level) [%t] [app=%property{appName}] %logger{36} - %msg%n</pattern>
    </else>
</if>
```

## Testing Colors

To test if colors are working, add this to your code:

```java
log.error("This is an ERROR message - should be RED");
log.warn("This is a WARN message - should be YELLOW");
log.info("This is an INFO message - should be GREEN");
log.debug("This is a DEBUG message - should be CYAN");
```

## Example Output

With colors enabled, you'll see:

```
22:10:15.123 ERROR [main] c.d.s.handler.PostgresHandler - Connection failed
22:10:15.234 WARN  [main] c.d.s.util.ShardPlanner - Using default shard count
22:10:15.345 INFO  [main] c.d.s.service.DataflowRunnerService - Pipeline started
22:10:15.456 DEBUG [worker-1] c.d.s.handler.PostgresHandler - Executing query
```

Where:
- `ERROR` appears in **red**
- `WARN` appears in **yellow**
- `INFO` appears in **green**
- `DEBUG` appears in **cyan**

## Summary

✅ **Colors are now enabled** using the `%highlight` converter
✅ **Automatic color mapping** for all log levels
✅ **Works in most modern terminals**
✅ **Can be disabled** via system property if needed

The configuration is ready to use! Colors will appear automatically when you run your application in a terminal that supports ANSI colors.
