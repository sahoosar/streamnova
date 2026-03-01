package com.di.streamnova.load.config;

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Registers a Google Cloud Storage client bean backed by
 * Application Default Credentials (ADC).
 *
 * <p>Used by Stage 1 (shard JSONL writes) and the manifest writer/reader.</p>
 */
@Configuration
public class GcsStorageConfig {

    @Bean
    @ConditionalOnMissingBean(Storage.class)
    public Storage gcsStorage() {
        return StorageOptions.getDefaultInstance().getService();
    }
}
