package com.di.streamnova.ai.agent;

import com.di.streamnova.ai.config.AiProperties;
import com.di.streamnova.ai.dto.DataQualityReport;
import com.di.streamnova.ai.tool.DataQualityTool;
import com.di.streamnova.ai.tool.ProfilerTool;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;

/**
 * Dedicated AI agent for data quality analysis.
 *
 * <p>When called, it autonomously:
 * <ol>
 *   <li>Profiles the table to understand its shape and volume</li>
 *   <li>Runs data quality checks (nulls, duplicates, outliers)</li>
 *   <li>Produces a structured quality report with AI-generated recommendations</li>
 * </ol>
 *
 * <p>This agent is intentionally separate from {@link PipelineAdvisorAgent} so
 * it can be called independently (e.g. on a schedule before each pipeline run).
 */
@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(name = "streamnova.ai.enabled", havingValue = "true", matchIfMissing = true)
public class DataQualityAgent {

    private final ChatClient       chatClient;
    private final AiProperties     aiProperties;
    private final DataQualityTool  dataQualityTool;
    private final ProfilerTool     profilerTool;

    @Value("classpath:prompts/data-quality-system.st")
    private Resource systemPromptResource;

    /**
     * Runs a full AI-powered data quality assessment for a table.
     *
     * @param sourceType  postgres | oracle | mysql | gcs | bigquery
     * @param schema      database schema name
     * @param tableName   table name
     * @param pkColumn    primary key column (used for duplicate detection)
     * @return structured quality report with AI summary and recommendations
     */
    public DataQualityReport assess(String sourceType, String schema,
                                    String tableName, String pkColumn) {
        log.info("[DQ-AGENT] Assessing {}.{} (source={})", schema, tableName, sourceType);
        long start = System.currentTimeMillis();

        String prompt = String.format("""
                Perform a comprehensive data quality assessment for table '%s.%s' \
                from source type '%s'. Primary key column is '%s'.
                1. Profile the table first.
                2. Run full data quality checks.
                3. Check null values in all critical columns.
                4. Provide a structured summary with: overall_quality_score (0-100), \
                   critical_issues (list), warnings (list), and recommendations (list).
                """, schema, tableName, sourceType, pkColumn);

        try {
            String aiSummary = chatClient.prompt()
                    .system(systemPromptResource)
                    .user(prompt)
                    .tools(dataQualityTool, profilerTool)
                    .call()
                    .content();

            long durationMs = System.currentTimeMillis() - start;
            log.info("[DQ-AGENT] Assessment complete for {}.{} in {}ms", schema, tableName, durationMs);

            return DataQualityReport.builder()
                    .schema(schema)
                    .tableName(tableName)
                    .sourceType(sourceType)
                    .aiSummary(aiSummary)
                    .durationMs(durationMs)
                    .success(true)
                    .build();

        } catch (Exception ex) {
            log.error("[DQ-AGENT] Assessment failed for {}.{}: {}", schema, tableName, ex.getMessage());
            return DataQualityReport.builder()
                    .schema(schema)
                    .tableName(tableName)
                    .sourceType(sourceType)
                    .success(false)
                    .errorMessage(ex.getMessage())
                    .build();
        }
    }
}
