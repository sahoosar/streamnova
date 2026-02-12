package com.di.streamnova.agent.execution_planner;

import com.di.streamnova.agent.profiler.ProfileResult;
import com.di.streamnova.agent.profiler.ProfileStore;
import com.di.streamnova.agent.profiler.ProfilerService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST API for the Candidate Generator segment.
 * Generates machine/worker/shard candidates from the latest profile (or from a profile run id).
 * Use with context path: e.g. GET /streamnova/api/agent/candidates/generate
 */
@RestController
@RequestMapping("/api/agent/candidates")
@RequiredArgsConstructor
public class AdaptivePlannerController {

    private final ProfilerService profilerService;
    private final AdaptiveExecutionPlannerService adaptiveExecutionPlannerService;
    private final ProfileStore profileStore;

    /**
     * Runs Profiler then generates candidates from the resulting table profile.
     *
     * @param source     optional source key (e.g. postgres)
     * @param warmUp     whether to run warm-up read during profile (default true)
     * @param maxCandidates cap on number of candidates (default 24)
     * @param machineFamily optional filter: n2, n2d, c3
     */
    @GetMapping(value = "/generate", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<AdaptivePlanResult> generate(
            @RequestParam(required = false) String source,
            @RequestParam(required = false, defaultValue = "true") boolean warmUp,
            @RequestParam(required = false) Integer maxCandidates,
            @RequestParam(required = false) String machineFamily) {
        ProfileResult profileResult = profilerService.profile(source, warmUp);
        if (profileResult == null || profileResult.getTableProfile() == null) {
            return ResponseEntity.noContent().build();
        }
        AdaptivePlanResult result = adaptiveExecutionPlannerService.generate(
                profileResult.getTableProfile(),
                profileResult.getRunId(),
                maxCandidates,
                machineFamily);
        return ResponseEntity.ok(result);
    }

    /**
     * Generates candidates from an existing profile run (by run id from Profiler store).
     * Requires the profile to be in the store (in-memory or persistent).
     */
    @GetMapping(value = "/generate-from-profile", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<AdaptivePlanResult> generateFromProfile(
            @RequestParam String runId,
            @RequestParam(required = false) Integer maxCandidates,
            @RequestParam(required = false) String machineFamily) {
        return profileStore.findByRunId(runId)
                .filter(pr -> pr.getTableProfile() != null)
                .map(pr -> adaptiveExecutionPlannerService.generate(
                        pr.getTableProfile(), pr.getRunId(), maxCandidates, machineFamily))
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }
}
