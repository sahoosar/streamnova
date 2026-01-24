package com.di.streamnova.config;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class IntermediateConfig {
    private String dataFormat;
    private String gcsPath;
}
