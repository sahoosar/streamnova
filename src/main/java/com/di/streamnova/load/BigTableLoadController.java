package com.di.streamnova.load;

import com.di.streamnova.load.dto.BigTableLoadRequest;
import com.di.streamnova.load.dto.BigTableLoadResponse;
import com.di.streamnova.load.metadata.LoadManifest;
import com.di.streamnova.load.metadata.LoadManifestRepository;
import com.di.streamnova.load.metadata.LoadRun;
import com.di.streamnova.load.metadata.LoadRunRepository;
import com.di.streamnova.load.metadata.LoadShard;
import com.di.streamnova.load.metadata.LoadShardRepository;
import com.di.streamnova.load.metadata.LoadValidation;
import com.di.streamnova.load.metadata.LoadValidationRepository;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * REST controller for the big-table load pipeline.
 *
 * <p><strong>Base path (relative to server context-path):</strong>
 * {@code /api/load}
 *
 * <table border="1">
 * <tr><th>Method</th><th>Path</th><th>Description</th></tr>
 * <tr><td>POST</td><td>/api/load/big-table</td>
 *     <td>Trigger a new 3-stage load run (synchronous)</td></tr>
 * <tr><td>GET</td><td>/api/load/runs/{runId}</td>
 *     <td>Fetch run record</td></tr>
 * <tr><td>GET</td><td>/api/load/runs/{runId}/shards</td>
 *     <td>List all shard rows for a run</td></tr>
 * <tr><td>GET</td><td>/api/load/runs/{runId}/manifests</td>
 *     <td>List manifest records for a run</td></tr>
 * <tr><td>GET</td><td>/api/load/runs/{runId}/validations</td>
 *     <td>List validation records for a run</td></tr>
 * <tr><td>GET</td><td>/api/load/runs?status=PROMOTED</td>
 *     <td>List runs filtered by status</td></tr>
 * </table>
 *
 * <p>The {@code POST /api/load/big-table} endpoint is <em>synchronous</em> —
 * it blocks until the run completes (or fails).  For large tables wrap the
 * orchestrator call with {@code @Async} or use the Spring AI agent execution
 * flow.
 */
@RestController
@RequestMapping("/api/load")
@Slf4j
@RequiredArgsConstructor
public class BigTableLoadController {

    private final BigTableLoadOrchestrator   orchestrator;
    private final LoadRunRepository          runRepo;
    private final LoadShardRepository        shardRepo;
    private final LoadManifestRepository     manifestRepo;
    private final LoadValidationRepository   validationRepo;

    /* ------------------------------------------------------------------ */
    /* Trigger                                                              */
    /* ------------------------------------------------------------------ */

    /**
     * Triggers a 3-stage Postgres → GCS → BigQuery load with adaptive
     * shard planning (up to 3 iterations).
     *
     * <p>Returns {@code 201 Created} on success with the run details,
     * or {@code 500} with an error message on failure.
     */
    @PostMapping("/big-table")
    public ResponseEntity<BigTableLoadResponse> triggerLoad(
            @Valid @RequestBody BigTableLoadRequest request) {

        log.info("[CONTROLLER] POST /api/load/big-table table={}.{} gcsBucket={} bqTable={}.{}.{}",
                 request.getTableSchema(), request.getTableName(),
                 request.getGcsBucket(),
                 request.getBqProject(), request.getBqDataset(), request.getBqTable());

        BigTableLoadResponse response = orchestrator.execute(request);

        HttpStatus status = "FAILED".equals(response.getStatus())
                ? HttpStatus.INTERNAL_SERVER_ERROR
                : HttpStatus.CREATED;

        return ResponseEntity.status(status).body(response);
    }

    /* ------------------------------------------------------------------ */
    /* Run queries                                                          */
    /* ------------------------------------------------------------------ */

    @GetMapping("/runs/{runId}")
    public ResponseEntity<LoadRun> getRun(@PathVariable String runId) {
        return runRepo.findById(runId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping("/runs")
    public List<LoadRun> listRuns(
            @RequestParam(required = false) String status,
            @RequestParam(required = false) String schema,
            @RequestParam(required = false) String table) {

        if (status != null) {
            return runRepo.findByStatus(status);
        }
        if (schema != null && table != null) {
            return runRepo.findByTable(schema, table);
        }
        // default: return recent PROMOTED runs
        return runRepo.findByStatus("PROMOTED");
    }

    /* ------------------------------------------------------------------ */
    /* Shard / manifest / validation details                               */
    /* ------------------------------------------------------------------ */

    @GetMapping("/runs/{runId}/shards")
    public List<LoadShard> getShards(@PathVariable String runId) {
        return shardRepo.findByRunId(runId);
    }

    @GetMapping("/runs/{runId}/manifests")
    public List<LoadManifest> getManifests(@PathVariable String runId) {
        return manifestRepo.findByRunId(runId);
    }

    @GetMapping("/runs/{runId}/validations")
    public List<LoadValidation> getValidations(@PathVariable String runId) {
        return validationRepo.findByRunId(runId);
    }

    /* ------------------------------------------------------------------ */
    /* Summary endpoint                                                     */
    /* ------------------------------------------------------------------ */

    /**
     * Returns a concise status summary for a run, including shard progress
     * counts.
     */
    @GetMapping("/runs/{runId}/summary")
    public ResponseEntity<Map<String, Object>> getSummary(@PathVariable String runId) {
        return runRepo.findById(runId)
                .map(run -> {
                    long done    = shardRepo.countByStatus(runId, "DONE");
                    long running = shardRepo.countByStatus(runId, "RUNNING");
                    long pending = shardRepo.countByStatus(runId, "PENDING");
                    long failed  = shardRepo.countByStatus(runId, "FAILED");
                    long total   = done + running + pending + failed;

                    Map<String, Object> summary = Map.of(
                        "runId",          run.getId(),
                        "status",         run.getStatus(),
                        "table",          run.getTableSchema() + "." + run.getTableName(),
                        "machineType",    run.getMachineType() != null ? run.getMachineType() : "",
                        "shards",         Map.of(
                            "total",   total,
                            "done",    done,
                            "running", running,
                            "pending", pending,
                            "failed",  failed
                        ),
                        "startedAt",      run.getStartedAt() != null ? run.getStartedAt().toString() : "",
                        "promotedAt",     run.getPromotedAt() != null ? run.getPromotedAt().toString() : ""
                    );
                    return ResponseEntity.ok(summary);
                })
                .orElse(ResponseEntity.notFound().build());
    }
}
