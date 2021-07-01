package com.gigaspaces.jdbc.calcite;

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.validate.SqlNameMatchers;

import java.util.List;

public class GSCalciteCatalogReader extends CalciteCatalogReader {
    public GSCalciteCatalogReader(
        CalciteSchema rootSchema,
        List<List<String>> searchPaths,
        RelDataTypeFactory typeFactory,
        CalciteConnectionConfig config
    ) {
        super(
            rootSchema,
            SqlNameMatchers.withCaseSensitive(config != null && config.caseSensitive()),
            searchPaths,
            typeFactory,
            config
        );
    }
}
