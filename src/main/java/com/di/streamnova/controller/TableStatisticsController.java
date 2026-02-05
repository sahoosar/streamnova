package com.di.streamnova.controller;

import com.di.streamnova.config.PipelineConfigSource;
import com.di.streamnova.config.YamlPipelineProperties;
import com.di.streamnova.controller.dto.TableStatisticsResponse;
import com.di.streamnova.handler.jdbc.postgres.PostgresStatisticsEstimator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST API to access table statistics (row count, average row size) before loading data.
 * Uses the pipeline source config from pipeline_config.yml.
 */
@Slf4j
@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class TableStatisticsController {

    private final YamlPipelineProperties yamlPipelineProperties;

    /**
     * Get table statistics for the configured PostgreSQL source.
     * Uses pipeline.config.source (jdbcUrl, table, etc.) from pipeline_config.yml.
     *
     * Example: GET /api/pipeline/table-statistics
     */
    @GetMapping(value = "/pipeline/table-statistics", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<TableStatisticsResponse> getTableStatistics() {
        PipelineConfigSource source = yamlPipelineProperties.getConfig().getSource();
        if (source == null) {
            return ResponseEntity.badRequest().build();
        }
        if (!"postgres".equalsIgnoreCase(source.getType())) {
            log.warn("[STATS] Table statistics only supported for type=postgres; current type={}", source.getType());
            return ResponseEntity.badRequest().build();
        }

        PostgresStatisticsEstimator.TableStatistics stats = PostgresStatisticsEstimator.estimateStatistics(source);

        TableStatisticsResponse body = TableStatisticsResponse.builder()
                .table(source.getTable())
                .rowCount(stats.rowCount())
                .avgRowSizeBytes(stats.avgRowSizeBytes())
                .build();

        return ResponseEntity.ok(body);
    }
}
