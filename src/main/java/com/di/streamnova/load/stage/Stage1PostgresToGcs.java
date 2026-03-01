package com.di.streamnova.load.stage;

import com.di.streamnova.load.manifest.GcsManifest;
import com.di.streamnova.load.metadata.LoadShard;
import com.di.streamnova.load.metadata.LoadShardRepository;
import com.di.streamnova.load.plan.ShardRange;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Stage 1 executor: reads all shards from Postgres and writes them as JSONL
 * to Google Cloud Storage.
 *
 * <h3>Concurrency model</h3>
 * <pre>
 *   maxConcurrent  = min(shardCount, poolSize)   [≤ 10 for 10-connection pool]
 *
 *   A fixed thread-pool of size maxConcurrent is used together with a
 *   Semaphore(maxConcurrent) so that at most poolSize JDBC connections are
 *   checked out simultaneously regardless of how the thread scheduler behaves.
 *
 *   Wave execution (pilot-aware):
 *     1. If iteration 3 is enabled, shard 0 is run alone first.
 *        Its duration is returned to the orchestrator for plan refinement.
 *     2. Remaining shards (1..n) are dispatched in the standard wave loop.
 * </pre>
 *
 * <h3>Manifest output</h3>
 * After all shards succeed, a {@link GcsManifest} is assembled from
 * {@code load_shards} rows and returned to the caller.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class Stage1PostgresToGcs {

    private final ShardReader          shardReader;
    private final LoadShardRepository  shardRepo;
    private final Storage              storage;

    /* ------------------------------------------------------------------ */
    /* Pilot shard (iteration 3 feedback)                                   */
    /* ------------------------------------------------------------------ */

    /**
     * Executes shard 0 synchronously and returns its wall-clock duration.
     * Called before the full batch when iteration 3 is active.
     */
    public long executePilotShard(String runId,
                                  ShardRange pilotShard,
                                  String schema, String table, String partitionCol,
                                  String gcsBucket, String gcsPrefix) {
        log.info("[STAGE1-PILOT] shard=0 range=[{},{}]",
                 pilotShard.getMinKey(), pilotShard.getMaxKey());
        long durationMs = executeSingleShard(
                runId, pilotShard, schema, table, partitionCol, gcsBucket, gcsPrefix);
        log.info("[STAGE1-PILOT] completed in {}ms", durationMs);
        return durationMs;
    }

    /* ------------------------------------------------------------------ */
    /* Main batch                                                           */
    /* ------------------------------------------------------------------ */

    /**
     * Executes all shards in {@code shards} in pool-limited parallel waves
     * and returns a {@link GcsManifest} describing the output files.
     *
     * @param shards         full shard list (may include the already-done pilot at index 0)
     * @param pilotAlreadyDone {@code true} when shard 0 was executed as a pilot
     */
    public GcsManifest executeBatch(
            String           runId,
            List<ShardRange> shards,
            String           schema,
            String           table,
            String           partitionCol,
            String           gcsBucket,
            String           gcsPrefix,
            int              maxConcurrent,
            boolean          pilotAlreadyDone) {

        int startIndex = pilotAlreadyDone ? 1 : 0;
        List<ShardRange> pending = shards.subList(startIndex, shards.size());

        log.info("[STAGE1] executing {} shards (maxConcurrent={}, pilotDone={})",
                 pending.size(), maxConcurrent, pilotAlreadyDone);

        if (!pending.isEmpty()) {
            runParallel(runId, pending, schema, table, partitionCol,
                        gcsBucket, gcsPrefix, maxConcurrent);
        }

        return buildManifest(runId, schema, table, gcsBucket, gcsPrefix,
                             shards.size(), pilotAlreadyDone);
    }

    /* ------------------------------------------------------------------ */
    /* Private: parallel wave execution                                     */
    /* ------------------------------------------------------------------ */

    /**
     * Executes all {@code shards} in parallel, capped at {@code maxConcurrent}
     * concurrent tasks (one JDBC connection per task).
     *
     * <h3>Concurrency</h3>
     * A {@code FixedThreadPool(maxConcurrent)} is the sole concurrency limit —
     * a {@code Semaphore} is superfluous because the pool itself guarantees that
     * at most {@code maxConcurrent} tasks run simultaneously.
     *
     * <h3>Error handling</h3>
     * Each shard future uses {@code exceptionally} to catch and record failures
     * while allowing the remaining shards to complete.  All collected errors
     * are surfaced in a single exception after {@code allOf} returns.
     */
    private void runParallel(String           runId,
                             List<ShardRange> shards,
                             String           schema,
                             String           table,
                             String           partitionCol,
                             String           gcsBucket,
                             String           gcsPrefix,
                             int              maxConcurrent) {

        ThreadFactory     tf       = r -> { var t = new Thread(r, "stage1-shard"); t.setDaemon(true); return t; };
        ExecutorService   executor = Executors.newFixedThreadPool(maxConcurrent, tf);

        // Collect per-shard failures without stopping other shards
        ConcurrentLinkedQueue<String> errors = new ConcurrentLinkedQueue<>();
        List<CompletableFuture<Void>>  futures = new ArrayList<>(shards.size());

        for (ShardRange shard : shards) {
            CompletableFuture<Void> f = CompletableFuture
                    .runAsync(
                            () -> executeSingleShard(runId, shard, schema, table,
                                                     partitionCol, gcsBucket, gcsPrefix),
                            executor)
                    // exceptionally: convert exceptional completion → normal null
                    // so that allOf() waits for ALL shards, not just the first failure
                    .exceptionally(ex -> {
                        String msg = String.format("shard-%d: %s",
                                                   shard.getShardIndex(), ex.getMessage());
                        errors.add(msg);
                        log.error("[STAGE1] {} FAILED: {}", msg, ex.getMessage());
                        return null;
                    });
            futures.add(f);
        }

        try {
            CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
                    .get(4, TimeUnit.HOURS);
        } catch (TimeoutException e) {
            throw new RuntimeException("Stage 1 timed out after 4 hours", e);
        } catch (ExecutionException e) {
            // Should not happen because exceptionally() absorbs failures above,
            // but guard defensively.
            throw new RuntimeException("Stage 1 execution error", e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Stage 1 interrupted", e);
        } finally {
            executor.shutdown();
        }

        if (!errors.isEmpty()) {
            throw new RuntimeException(String.format(
                    "Stage 1 failed: %d/%d shard(s) → %s",
                    errors.size(), shards.size(), String.join(" | ", errors)));
        }
    }

    /* ------------------------------------------------------------------ */
    /* Private: single shard — delegates to ShardReader                    */
    /* ------------------------------------------------------------------ */

    /**
     * @return wall-clock duration in milliseconds
     */
    private long executeSingleShard(String runId, ShardRange shard,
                                    String schema, String table, String partitionCol,
                                    String gcsBucket, String gcsPrefix) {

        String gcsObjectPath = gcsPrefix + "/stage1/shard_"
                + String.format("%06d", shard.getShardIndex()) + ".jsonl";

        shardRepo.markRunning(runId, shard.getShardIndex(), "gs://" + gcsBucket + "/" + gcsObjectPath);

        long startMs = System.currentTimeMillis();
        try {
            long rowCount = shardReader.readAndWrite(
                    shard, schema, table, partitionCol, gcsBucket, gcsObjectPath);
            long durationMs = System.currentTimeMillis() - startMs;

            // Retrieve GCS file size
            Blob blob     = storage.get(BlobId.of(gcsBucket, gcsObjectPath));
            long fileSize = blob != null ? blob.getSize() : -1L;

            shardRepo.markDone(runId, shard.getShardIndex(), rowCount, fileSize, durationMs);
            log.info("[STAGE1] shard={} rows={} size={}B duration={}ms",
                     shard.getShardIndex(), rowCount, fileSize, durationMs);
            return durationMs;

        } catch (Exception ex) {
            long durationMs = System.currentTimeMillis() - startMs;
            shardRepo.markFailed(runId, shard.getShardIndex(), ex.getMessage(), durationMs);
            throw new RuntimeException("Shard " + shard.getShardIndex() + " failed", ex);
        }
    }

    /* ------------------------------------------------------------------ */
    /* Private: build manifest from completed shard rows                    */
    /* ------------------------------------------------------------------ */

    private GcsManifest buildManifest(String runId,
                                      String schema, String table,
                                      String gcsBucket, String gcsPrefix,
                                      int expectedShards,
                                      boolean pilotIncluded) {

        List<LoadShard> doneShards = shardRepo.findDoneByRunId(runId);

        if (doneShards.size() != expectedShards) {
            log.warn("[STAGE1] expected {} DONE shards, found {}",
                     expectedShards, doneShards.size());
        }

        List<GcsManifest.Entry> entries = new ArrayList<>(doneShards.size());
        long totalRows  = 0;
        long totalBytes = 0;

        for (LoadShard s : doneShards) {
            long rows  = s.getActualRowCount()   != null ? s.getActualRowCount()   : 0L;
            long bytes = s.getGcsFileSizeBytes() != null ? s.getGcsFileSizeBytes() : 0L;
            totalRows  += rows;
            totalBytes += bytes;
            entries.add(GcsManifest.Entry.builder()
                    .shardIndex(s.getShardIndex())
                    .gcsPath(s.getGcsPath())
                    .rowCount(rows)
                    .sizeBytes(bytes)
                    .build());
        }

        entries.sort(Comparator.comparingInt(GcsManifest.Entry::getShardIndex));

        return GcsManifest.builder()
                .runId(runId)
                .stage("STAGE1")
                .tableSchema(schema)
                .tableName(table)
                .createdAt(Instant.now())
                .totalFiles(entries.size())
                .totalRows(totalRows)
                .totalBytes(totalBytes)
                .files(entries)
                .build();
    }
}
