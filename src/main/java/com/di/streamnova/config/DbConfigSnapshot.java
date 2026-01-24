package com.di.streamnova.config;

import java.io.Serializable;

public record DbConfigSnapshot(String jdbcUrl, String username, String password, String driverClassName,
                               int maximumPoolSize, int minimumIdle, long idleTimeoutMs, long connectionTimeoutMs,
                               long maxLifetimeMs) implements Serializable {
}
