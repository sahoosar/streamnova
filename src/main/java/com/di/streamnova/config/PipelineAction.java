package com.di.streamnova.config;

/**
 * Pipeline loading actions. Used to branch by 2-stage vs 3-stage and to wire event listener config.
 * <ul>
 *   <li>2-stage: SOURCE_READ, then TARGET_WRITE.</li>
 *   <li>3-stage: SOURCE_READ, then INTERMEDIATE_WRITE, then TARGET_WRITE.</li>
 * </ul>
 */
public enum PipelineAction {
    SOURCE_READ,
    INTERMEDIATE_WRITE,
    TARGET_WRITE
}
