package com.di.streamnova.sharding.api;

import java.util.List;

/** Result of selection. */
public record ColumnSelection(List<String> columns, List<ColumnStat> considered, String note) {}
