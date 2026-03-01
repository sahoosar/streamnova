package com.di.streamnova.load.validation;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

/**
 * Row-count reconciliation between pipeline stages.
 *
 * <p>Two checks are performed during each load run:
 * <ol>
 *   <li><strong>AFTER_STAGE1</strong>: GCS manifest total rows vs
 *       {@code SELECT COUNT(*)} from the source Postgres table.</li>
 *   <li><strong>AFTER_STAGE3</strong>: BigQuery table row count vs
 *       GCS manifest total rows.</li>
 * </ol>
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class RowCountValidator {

    private final JdbcTemplate jdbcTemplate;
    private final BigQuery     bigQuery;

    /* ------------------------------------------------------------------ */
    /* After Stage 1 — manifest vs Postgres COUNT(*)                       */
    /* ------------------------------------------------------------------ */

    /**
     * Validates that the sum of rows written to GCS (from the manifest)
     * matches the live Postgres row count within the given threshold.
     *
     * @param schema       source schema
     * @param table        source table
     * @param manifestRows total rows recorded in the Stage-1 manifest
     * @param thresholdPct acceptable deviation (e.g. 0.01 = 0.01 %)
     */
    public ValidationResult validateStage1(
            String schema, String table,
            long   manifestRows,
            double thresholdPct) {

        long sourceCount = countPostgres(schema, table);
        return reconcile("AFTER_STAGE1", sourceCount, manifestRows, thresholdPct);
    }

    /* ------------------------------------------------------------------ */
    /* After Stage 3 — BigQuery row count vs manifest                      */
    /* ------------------------------------------------------------------ */

    /**
     * Validates that the BigQuery table row count matches the manifest total.
     *
     * @param bqProject    BigQuery billing project
     * @param bqDataset    dataset
     * @param bqTable      table
     * @param manifestRows total rows from the Stage-1 manifest
     * @param thresholdPct acceptable deviation
     */
    public ValidationResult validateStage3(
            String bqProject, String bqDataset, String bqTable,
            long   manifestRows,
            double thresholdPct) {

        long bqCount = countBigQuery(bqProject, bqDataset, bqTable);
        return reconcile("AFTER_STAGE3", manifestRows, bqCount, thresholdPct);
    }

    /* ------------------------------------------------------------------ */
    /* Internal helpers                                                     */
    /* ------------------------------------------------------------------ */

    private ValidationResult reconcile(
            String stage,
            long   expected,
            long   actual,
            double thresholdPct) {

        long   delta    = actual - expected;
        double deltaPct = expected == 0 ? 0.0
                        : Math.abs(delta) / (double) expected * 100.0;
        boolean passed  = deltaPct <= thresholdPct;

        String detail = String.format(
                "%s: expected=%,d actual=%,d delta=%+,d (%.4f %% vs threshold %.4f %%) → %s",
                stage, expected, actual, delta, deltaPct, thresholdPct,
                passed ? "PASS" : "FAIL");

        log.info("[VALIDATE] {}", detail);

        return ValidationResult.builder()
                .stage(stage)
                .expectedRows(expected)
                .actualRows(actual)
                .deltaRows(delta)
                .deltaPct(deltaPct)
                .thresholdPct(thresholdPct)
                .passed(passed)
                .detail(detail)
                .build();
    }

    private long countPostgres(String schema, String table) {
        String sql  = "SELECT COUNT(*) FROM \"" + schema + "\".\"" + table + "\"";
        Long   cnt  = jdbcTemplate.queryForObject(sql, Long.class);
        long   result = cnt == null ? 0L : cnt;
        log.debug("[VALIDATE] Postgres count {}.{} = {}", schema, table, result);
        return result;
    }

    private long countBigQuery(String project, String dataset, String table) {
        String sql = String.format("SELECT COUNT(*) AS cnt FROM `%s.%s.%s`",
                                   project, dataset, table);
        try {
            QueryJobConfiguration cfg = QueryJobConfiguration.newBuilder(sql)
                    .setUseLegacySql(false)
                    .build();
            TableResult result = bigQuery.query(cfg);
            long cnt = result.iterateAll().iterator().next().get("cnt").getLongValue();
            log.debug("[VALIDATE] BigQuery count {}.{}.{} = {}", project, dataset, table, cnt);
            return cnt;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("BQ count interrupted for " + project + "." + dataset + "." + table, e);
        } catch (Exception e) {
            throw new RuntimeException("BQ count failed for " + project + "." + dataset + "." + table, e);
        }
    }
}
