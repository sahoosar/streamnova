package com.di.streamnova.tuning;

import lombok.extern.slf4j.Slf4j;

import java.util.Locale;

@Slf4j
public final class EnvironmentDetector {
    private EnvironmentDetector() {}

    /** Returns "gcp", "aws", "mac", "windows", or "linux". */
    public static String detect() {
        var env = System.getenv();
        if (env.containsKey("DATAFLOW_JOB_ID") || env.containsKey("GOOGLE_CLOUD_PROJECT") || env.containsKey("GCP_PROJECT"))
            return "gcp";
        if (env.containsKey("AWS_EXECUTION_ENV") || env.containsKey("ECS_CONTAINER_METADATA_URI")
                || env.containsKey("LAMBDA_TASK_ROOT") || env.containsKey("EC2_INSTANCE_ID"))
            return "aws";

        String os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
        if (os.contains("mac")) return "mac";
        if (os.contains("win")) return "windows";
        return "linux";
    }
}
