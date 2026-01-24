package com.di.streamnova.sharding.factory;

import com.di.streamnova.sharding.api.CardinalityModel;
import com.di.streamnova.sharding.history.HistoryModel;
import com.di.streamnova.sharding.oracle.OracleStatsCardinalityModel;
import com.di.streamnova.sharding.postgres.PgStatsCardinalityModel;
import com.di.streamnova.sharding.range.RangeModel;
import com.di.streamnova.sharding.sampler.SkewAwareCardinalityModel;

public class ShardModelFactory {
    public static CardinalityModel forSource(String sourceType, String mode, int numPartitions) {
     /*   switch ((mode == null ? "pg_stats" : mode)) {
            case "pg_stats"      -> { return "oracle".equalsIgnoreCase(sourceType)
                    ? new OracleStatsCardinalityModel()
                    : new PgStatsCardinalityModel(); }
            case "skew-sampler"  -> { return new SkewAwareCardinalityModel(numPartitions, 1.0, 48, 3, 50); }
            case "range"         -> { return new RangeModel(); }
            case "history-or(pg_stats)" -> { return new HistoryModel(*//*deps*//*); }
            default              -> { return new PgStatsCardinalityModel(); }
        }
    }*/
        return null;
    }
}