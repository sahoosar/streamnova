package com.di.streamnova.load.manifest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

/**
 * Serialises a {@link GcsManifest} to JSON and uploads it to GCS.
 */
@Component
@Slf4j
public class ManifestWriter {

    private final Storage      storage;
    private final ObjectMapper objectMapper;

    public ManifestWriter(Storage storage) {
        this.storage      = storage;
        this.objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    /**
     * Writes the manifest JSON to
     * {@code gs://{bucket}/{prefix}/{stage}/manifest.json}
     * and returns the full GCS path.
     */
    public String write(String bucket, String prefix, GcsManifest manifest) {
        String stage      = manifest.getStage().toLowerCase();
        String objectName = prefix + "/" + stage + "/manifest.json";
        String gcsPath    = "gs://" + bucket + "/" + objectName;

        try {
            byte[]   json     = objectMapper.writeValueAsBytes(manifest);
            BlobId   blobId   = BlobId.of(bucket, objectName);
            BlobInfo blobInfo = BlobInfo.newBuilder(blobId)
                    .setContentType("application/json")
                    .build();

            storage.create(blobInfo, json);

            log.info("[MANIFEST] written {} files / {} rows → {}",
                     manifest.getTotalFiles(), manifest.getTotalRows(), gcsPath);
            return gcsPath;

        } catch (Exception e) {
            throw new RuntimeException("Failed to write manifest to " + gcsPath, e);
        }
    }
}
