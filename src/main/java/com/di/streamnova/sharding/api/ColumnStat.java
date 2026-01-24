package com.di.streamnova.sharding.api;

/** Lightweight stats record. */
public record ColumnStat(String name, double estDistinct, double nullFrac, boolean inPk) {}
