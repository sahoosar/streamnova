package com.di.streamnova.handler.config;

import java.util.Map;

public interface SourceConfig {
    String getType();
    Map<String, Object> getProperties();
}
