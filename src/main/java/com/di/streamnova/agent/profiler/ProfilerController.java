package com.di.streamnova.agent.profiler;

import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST API for the Profiler segment of the autonomous batch agent.
 * Use with context path: e.g. GET /streamnova/api/agent/profiler/profile
 */
@RestController
@RequestMapping("/api/agent/profiler")
@RequiredArgsConstructor
public class ProfilerController {

    private final ProfilerService profilerService;
    private final ProfileStore profileStore;

    /**
     * Runs full profiling: table stats (row count, avg row size) + warm-up read for throughput.
     *
     * @param source   optional source key (e.g. postgres); default from pipeline config
     * @param warmUp   optional; default true (run warm-up read)
     * @return profile result with table profile and optional throughput sample
     */
    @GetMapping(value = "/profile", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<ProfileResult> profile(
            @RequestParam(required = false) String source,
            @RequestParam(required = false, defaultValue = "true") boolean warmUp) {
        ProfileResult result = profilerService.profile(source, warmUp);
        if (result == null) {
            return ResponseEntity.noContent().build();
        }
        return ResponseEntity.ok(result);
    }

    /**
     * Returns the most recent profile results for a table (from in-memory or future persistent store).
     */
    @GetMapping(value = "/recent", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<java.util.List<ProfileResult>> recent(
            @RequestParam(required = false) String sourceType,
            @RequestParam(required = false) String schemaName,
            @RequestParam(required = false) String tableName,
            @RequestParam(required = false, defaultValue = "10") int limit) {
        java.util.List<ProfileResult> list = profileStore.findRecentByTable(
                sourceType, schemaName, tableName, Math.min(limit, 100));
        return ResponseEntity.ok(list);
    }
}
