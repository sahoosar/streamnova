package com.di.streamnova.controller;

import com.di.streamnova.config.PipelineConfigService;
import com.di.streamnova.config.PoolInitFailureRecorder;
import com.di.streamnova.controller.dto.PoolStatisticsWithDescriptions;
import com.di.streamnova.util.HikariDataSource;
import com.di.streamnova.util.PoolStatsSnapshot;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Pipeline config and pool statistics.
 * With context-path=/streamnova use: GET /streamnova/api/pipeline/pool-statistics
 */

@RestController
@RequestMapping("/api/pipeline")
@RequiredArgsConstructor
public class PipelineConfigController {

    private final PipelineConfigService pipelineConfigService;

    /**
     * Connection pool statistics. When no datasource has been created, returns 503 with error/sourceErrors in body.
     * Source keys for labelling are derived from config when available (otherwise pool names only).
     */
    @GetMapping(value = "/pool-statistics", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<PoolStatisticsWithDescriptions> getPoolStatistics() {
        List<PoolStatsSnapshot> raw = HikariDataSource.INSTANCE.getPoolStats();
        Map<String, String> sourceErrors = PoolInitFailureRecorder.getAllFailures();
        if (raw == null || raw.isEmpty()) {
            String reason = pipelineConfigService.getReasonNoPools(PoolInitFailureRecorder.getAllFailuresFormatted());
            return ResponseEntity.status(503).body(PoolStatisticsWithDescriptions.from(raw, List.of(), reason, sourceErrors));
        }
        List<String> sourceKeysInOrder = getSourceKeysForDisplay();
        return ResponseEntity.ok(PoolStatisticsWithDescriptions.from(raw, sourceKeysInOrder, null, sourceErrors));
    }

    /** Derives source keys from config when available (for pool labels); otherwise empty list. */
    private List<String> getSourceKeysForDisplay() {
        Set<String> keys = pipelineConfigService.getAvailableSourceKeys();
        if (keys == null || keys.isEmpty()) return List.of();
        return keys.stream().sorted().collect(Collectors.toList());
    }
}
