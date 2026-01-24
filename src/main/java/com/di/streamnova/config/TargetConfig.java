package com.di.streamnova.config;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class TargetConfig {
    private String type;
    private String dataset;
    private String table;
    private String schemaPath;
    private String loadMode;
    private String sourceFormat;
    private String writeDisposition;

}
