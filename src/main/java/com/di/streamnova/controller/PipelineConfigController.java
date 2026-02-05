package com.di.streamnova.controller;

import com.di.streamnova.config.PipelineConfigService;
import com.di.streamnova.config.PoolInitFailureRecorder;
import com.di.streamnova.controller.dto.PoolStatisticsWithDescriptions;
import com.di.streamnova.util.HikariDataSource;
import com.di.streamnova.util.PoolStatsSnapshot;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
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

    @Value("${streamnova.statistics.supported-source-types:postgres}")
    private String supportedSourceTypesConfig;

    /**
     * Connection pool statistics. When no datasource has been created, returns 503 with error/sourceErrors in body.
     * Full URL with context path: /streamnova/api/pipeline/pool-statistics
     */
    @GetMapping(value = "/pool-statistics", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<PoolStatisticsWithDescriptions> getPoolStatistics() {
        List<PoolStatsSnapshot> raw = HikariDataSource.INSTANCE.getPoolStats();
        Map<String, String> sourceErrors = PoolInitFailureRecorder.getAllFailures();
        if (raw == null || raw.isEmpty()) {
            String reason = pipelineConfigService.getReasonNoPools(PoolInitFailureRecorder.getAllFailuresFormatted());
            return ResponseEntity.status(503).body(PoolStatisticsWithDescriptions.from(raw, List.of(), reason, sourceErrors));
        }
        List<String> types = parseSupportedTypesList(supportedSourceTypesConfig);
        return ResponseEntity.ok(PoolStatisticsWithDescriptions.from(raw, types, null, sourceErrors));
    }

    private static List<String> parseSupportedTypesList(String config) {
        if (config == null || config.isBlank()) return List.of("postgres");
        return Arrays.stream(config.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .map(String::toLowerCase)
                .distinct()
                .collect(Collectors.toList());
    }
}
