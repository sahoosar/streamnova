package com.di.streamnova.sharding.oracle;

import com.di.streamnova.sharding.api.CardinalityModel;
import com.di.streamnova.sharding.api.ColumnSelection;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public class OracleStatsCardinalityModel implements CardinalityModel {
    @Override
    public ColumnSelection pickShardColumns(Connection con, String schema, String table, List<String> allowlist, int k, long minDistinct, double maxNullFrac, boolean preferPk) throws SQLException {
        return null;
    }
}
