package com.gigaspaces.jdbc;

import com.gigaspaces.jdbc.calcite.CalciteDefaults;
import com.gigaspaces.jdbc.calcite.handlers.CalciteQueryHandler;
import com.gigaspaces.jdbc.jsql.handlers.JsqlQueryHandler;
import com.j_spaces.core.IJSpace;
import com.j_spaces.jdbc.ResponsePacket;

import java.sql.SQLException;
import java.util.Properties;

public class QueryHandler {


    public ResponsePacket handle(String query, IJSpace space, Object[] preparedValues) throws SQLException {
        Properties customProperties = space.getURL().getCustomProperties();
        if (CalciteDefaults.isCalciteDriverPropertySet(customProperties)) {
            return new CalciteQueryHandler().handle(query, space, preparedValues);
        } else { //else jsql
            return new JsqlQueryHandler().handle(query, space, preparedValues);
        }
    }

}
