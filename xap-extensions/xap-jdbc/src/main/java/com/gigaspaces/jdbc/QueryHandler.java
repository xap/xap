package com.gigaspaces.jdbc;

import com.j_spaces.core.IJSpace;
import com.j_spaces.jdbc.ResponsePacket;

import java.sql.SQLException;

public class QueryHandler {

    public ResponsePacket handle(String query, IJSpace space, Object[] preparedValues) throws SQLException {
        return new JSqlQueryHandler().handle(query, space, preparedValues);
    }
}
