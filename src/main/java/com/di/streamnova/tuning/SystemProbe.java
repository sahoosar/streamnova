package com.di.streamnova.tuning;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class SystemProbe {

    private SystemProbe() {}

    /** Logical processors (override with -Dtuner.vcpus or env TUNER_VCPUS). */
    public static int detectVCpus() {
        String o = System.getProperty("tuner.vcpus", System.getenv("TUNER_VCPUS"));
        if (o != null && !o.isBlank()) {
            try {
                return Math.max(1, Integer.parseInt(o.trim()));
            } catch (Exception ignored) {} }
        return Math.max(1, Runtime.getRuntime().availableProcessors());
    }

    /** JVM max heap bytes (override with -Dtuner.heap.bytes or env TUNER_HEAP_BYTES). */
    public static long detectJvmHeapBytes() {
        String o = System.getProperty("tuner.heap.bytes", System.getenv("TUNER_HEAP_BYTES"));
        if (o != null && !o.isBlank()) {
            try {
                return Math.max(64L<<20, Long.parseLong(o.trim()));
            } catch (Exception ignored) {} }
        return Runtime.getRuntime().maxMemory();
    }
}

