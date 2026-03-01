package com.di.streamnova.load.config;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Registers a BigQuery client bean backed by Application Default Credentials.
 *
 * <p>Used by Stage 3 (GCS → BigQuery Load Job).</p>
 */
@Configuration
public class BigQueryClientConfig {

    @Bean
    @ConditionalOnMissingBean(BigQuery.class)
    public BigQuery bigQueryClient() {
        return BigQueryOptions.getDefaultInstance().getService();
    }
}
