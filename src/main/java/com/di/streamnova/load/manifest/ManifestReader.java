package com.di.streamnova.load.manifest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Reads and deserialises a {@link GcsManifest} from GCS.
 */
@Component
@Slf4j
public class ManifestReader {

    private final Storage      storage;
    private final ObjectMapper objectMapper;

    public ManifestReader(Storage storage) {
        this.storage      = storage;
        this.objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    /**
     * Parses the manifest JSON stored at {@code gs://{bucket}/{objectName}}.
     *
     * @param bucket     GCS bucket
     * @param objectName object key (without {@code gs://} prefix)
     */
    public GcsManifest read(String bucket, String objectName) {
        BlobId blobId = BlobId.of(bucket, objectName);
        Blob   blob   = storage.get(blobId);
        if (blob == null || !blob.exists()) {
            throw new IllegalStateException("Manifest not found: gs://" + bucket + "/" + objectName);
        }
        try {
            byte[] bytes = blob.getContent();
            log.debug("[MANIFEST-READ] {} bytes from gs://{}/{}", bytes.length, bucket, objectName);
            return objectMapper.readValue(bytes, GcsManifest.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to read manifest gs://" + bucket + "/" + objectName, e);
        }
    }

    /**
     * Convenience overload that accepts a full GCS URI
     * ({@code gs://bucket/object...}).
     */
    public GcsManifest readFromUri(String gcsUri) {
        if (!gcsUri.startsWith("gs://")) {
            throw new IllegalArgumentException("URI must start with gs://: " + gcsUri);
        }
        String withoutScheme = gcsUri.substring(5);            // strip "gs://"
        int    slash         = withoutScheme.indexOf('/');
        if (slash < 0) {
            throw new IllegalArgumentException("Invalid GCS URI: " + gcsUri);
        }
        String bucket     = withoutScheme.substring(0, slash);
        String objectName = withoutScheme.substring(slash + 1);
        return read(bucket, objectName);
    }
}
