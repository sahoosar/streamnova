package com.di.streamnova.load.stage;

import com.di.streamnova.load.plan.ShardRange;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Reads a single shard from Postgres and streams it as JSONL to GCS.
 *
 * <p>Lives in its own Spring bean so that {@code @Transactional(readOnly=true)}
 * is honoured (self-invocation from {@link Stage1PostgresToGcs} would bypass
 * the proxy).
 *
 * <p>The {@code @Transactional} annotation causes Spring to:
 * <ol>
 *   <li>Check out a HikariCP connection,</li>
 *   <li>Set {@code autoCommit = false}, which enables the PostgreSQL
 *       server-side streaming cursor when {@code fetchSize > 0},</li>
 *   <li>Hold the connection for the entire method scope.</li>
 * </ol>
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class ShardReader {

    /** JDBC fetch size: rows fetched per network round-trip. */
    private static final int  FETCH_SIZE     = 1_000;
    /** GCS write-channel flush interval in rows. */
    private static final int  WRITE_BUFFER   = 5_000;

    private final JdbcTemplate jdbcTemplate;
    private final Storage      storage;

    /**
     * Reads all rows in the shard range using a server-side cursor and writes
     * them as JSONL to the supplied GCS blob.
     *
     * @return actual row count written
     */
    @Transactional(readOnly = true)
    public long readAndWrite(ShardRange shard,
                             String     schema,
                             String     table,
                             String     partitionCol,
                             String     gcsBucket,
                             String     gcsObjectPath) {

        String sql = "SELECT * FROM " + qi(schema) + "." + qi(table)
                   + " WHERE " + qi(partitionCol) + " >= ? AND " + qi(partitionCol) + " <= ?"
                   + " ORDER BY " + qi(partitionCol);

        BlobId   blobId   = BlobId.of(gcsBucket, gcsObjectPath);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId)
                .setContentType("application/x-ndjson")
                .build();

        AtomicLong rowCount = new AtomicLong(0);

        try (var writer = storage.writer(blobInfo)) {
            StringBuilder buf = new StringBuilder(WRITE_BUFFER * 200);

            /*
             * Use ResultSetExtractor (not RowCallbackHandler) so we can read
             * ResultSetMetaData ONCE before the row loop, avoiding a per-row
             * metadata call.  Column labels are cached in a String[] array.
             */
            jdbcTemplate.query(
                con -> {
                    var ps = con.prepareStatement(sql,
                            ResultSet.TYPE_FORWARD_ONLY,
                            ResultSet.CONCUR_READ_ONLY);
                    ps.setLong(1, shard.getMinKey());
                    ps.setLong(2, shard.getMaxKey());
                    ps.setFetchSize(FETCH_SIZE);
                    return ps;
                },
                (ResultSet rs) -> {
                    // Cache column metadata once — not per row
                    ResultSetMetaData meta   = rs.getMetaData();
                    int               cols   = meta.getColumnCount();
                    String[]          labels = new String[cols];
                    for (int i = 0; i < cols; i++) {
                        labels[i] = meta.getColumnLabel(i + 1);
                    }

                    while (rs.next()) {
                        try {
                            buf.append('{');
                            for (int i = 0; i < cols; i++) {
                                if (i > 0) buf.append(',');
                                buf.append('"');
                                appendEscaped(buf, labels[i]);
                                buf.append("\":");
                                appendJsonValue(buf, rs.getObject(i + 1));
                            }
                            buf.append("}\n");

                            long n = rowCount.incrementAndGet();
                            if (n % WRITE_BUFFER == 0) {
                                flush(writer, buf);
                            }
                        } catch (IOException ex) {
                            throw new UncheckedIOException(ex);
                        }
                    }

                    // Flush remaining buffered rows after the loop
                    try {
                        if (!buf.isEmpty()) {
                            flush(writer, buf);
                        }
                    } catch (IOException ex) {
                        throw new UncheckedIOException(ex);
                    }
                    return null;  // ResultSetExtractor<Void>
                });

        } catch (IOException e) {
            throw new UncheckedIOException("GCS write failed for shard " + shard.getShardIndex(), e);
        }

        return rowCount.get();
    }

    /* ------------------------------------------------------------------ */

    private static void flush(com.google.cloud.storage.WriteChannel writer,
                               StringBuilder buf) throws IOException {
        byte[] bytes = buf.toString().getBytes(StandardCharsets.UTF_8);
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        while (bb.hasRemaining()) {
            writer.write(bb);
        }
        buf.setLength(0);
    }

    /** Minimal JSON-safe string escaping (handles the common cases). */
    private static void appendEscaped(StringBuilder sb, String s) {
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"'  -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default   -> sb.append(c);
            }
        }
    }

    private static void appendJsonValue(StringBuilder sb, Object val) {
        if (val == null) {
            sb.append("null");
        } else if (val instanceof Number || val instanceof Boolean) {
            sb.append(val);
        } else {
            sb.append('"');
            appendEscaped(sb, val.toString());
            sb.append('"');
        }
    }

    private static String qi(String name) {
        return "\"" + name.replace("\"", "\"\"") + "\"";
    }
}
