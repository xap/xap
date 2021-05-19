package com.gigaspaces.internal.server.space.tiered_storage;

import java.io.Closeable;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class RDBMSResult implements Closeable {

    private final Statement statement;
    private final ResultSet resultSet;

    public ResultSet getResultSet() {
        return resultSet;
    }

    public RDBMSResult(Statement statement, ResultSet resultSet) {
        this.statement = statement;
        this.resultSet = resultSet;
    }


    @Override
    public void close() throws IOException{
        try {
            if (statement != null) {
                statement.close();
            }
            if (resultSet != null) {
                resultSet.close();
            }
        } catch (SQLException e){
            throw new IOException(e);
        }
    }

    public boolean next() throws SQLException {
        return resultSet.next();
    }
}
