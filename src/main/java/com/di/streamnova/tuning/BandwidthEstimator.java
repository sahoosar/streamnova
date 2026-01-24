package com.di.streamnova.tuning;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class BandwidthEstimator {
    private BandwidthEstimator() {}

    /** Sustained per-connection bandwidth (bytes/sec); override with -Dtuner.bandwidth.bps or env. */
    public static long estimate(String environment) {
        String o = System.getProperty("tuner.bandwidth.bps", System.getenv("TUNER_BANDWIDTH_BPS"));
        if (o != null && !o.isBlank()) { try { return Math.max(10L<<20, Long.parseLong(o.trim())); } catch (Exception ignored) {} }
        switch (environment) {
            case "gcp":
            case "aws":   return 100L << 20; // ~100 MB/s
            default:      return 150L << 20; // local/LAN baseline
        }
    }
}
