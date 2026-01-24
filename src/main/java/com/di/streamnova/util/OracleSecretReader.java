package com.di.streamnova.util;

import com.google.cloud.secretmanager.v1.AccessSecretVersionResponse;
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
import com.google.cloud.secretmanager.v1.SecretVersionName;


public class OracleSecretReader {

    /*public static OracleConnectionProperties getOracleProperties(String projectId) {
        try (SecretManagerServiceClient client = SecretManagerServiceClient.create()) {
            String jdbcUrl  = getSecret(client, projectId, "oracle-jdbc-url");
            String username = getSecret(client, projectId, "oracle-username");
            String password = getSecret(client, projectId, "oracle-password");

            return new OracleConnectionProperties(jdbcUrl, username, password);
        } catch (Exception e) {
            throw new RuntimeException("Failed to read Oracle DB secrets", e);
        }
    }

    private static String getSecret(SecretManagerServiceClient client, String projectId, String secretId) {
        SecretVersionName secretVersionName = SecretVersionName.of(projectId, secretId, "latest");
        AccessSecretVersionResponse response = client.accessSecretVersion(secretVersionName);
        return response.getPayload().getData().toStringUtf8();
    }*/

}
