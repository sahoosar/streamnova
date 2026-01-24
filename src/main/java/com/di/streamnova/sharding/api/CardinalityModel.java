package com.di.streamnova.sharding.api;


import java.sql.*;
import java.util.*;

public interface CardinalityModel {
    ColumnSelection pickShardColumns(
            Connection con,
            String schema,
            String table,
            List<String> allowlist,  // null/empty → consider all string/number cols
            int k,                   // how many to pick (e.g., 3)
            long minDistinct,        // ignore near-constant columns (e.g., 100)
            double maxNullFrac,      // ignore very-null columns (e.g., 0.98)
            boolean preferPk         // pick PK columns if present
    ) throws SQLException;
}
