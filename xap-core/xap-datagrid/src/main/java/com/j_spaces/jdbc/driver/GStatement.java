/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.j_spaces.jdbc.driver;

import com.gigaspaces.logger.Constants;
import com.j_spaces.jdbc.ExplainPlanResponsePacket;
import com.j_spaces.jdbc.ResponsePacket;
import com.j_spaces.jdbc.ResultEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * The Statement implementation using a GConnection and a GResultSet
 *
 * @author Michael Mitrani, 2Train4
 */
@com.gigaspaces.api.InternalApi
public class GStatement implements Statement {

    protected final GConnection connection; //the defined connection for this statement
    protected int updateCount = -1; ///default or no results.
    protected ResultSet resultSet = null;
    protected List<String> _queriesBatch;
    protected boolean ignoreUnsupportedOptions;

    //logger
    final private static Logger _logger = LoggerFactory.getLogger(Constants.LOGGER_QUERY);

    //user configured behavior with unsupported sql operations

    final public static String IGNORE_UNSUPPORTED_OPTIONS_PROP = "com.gigaspaces.jdbc.ignoreUnsupportedOptions";

    public GStatement(GConnection connection) {
        this.connection = connection;
        ignoreUnsupportedOptions = Boolean.parseBoolean(System.getProperty(IGNORE_UNSUPPORTED_OPTIONS_PROP,"false"));
    }

    /**
     * Only ResultSet.FETCH_FORWARD is supported
     *
     * @see java.sql.Statement#getFetchDirection()
     */
    public int getFetchDirection() throws SQLException {
        return ResultSet.FETCH_UNKNOWN;
    }

    /**
     * this is just a hint method, so pay no attention to the returned value
     *
     * @see java.sql.Statement#getFetchSize()
     */
    public int getFetchSize() throws SQLException {
        return 0;
    }

    /**
     * Max field size. No limit in this case.
     *
     * @see java.sql.Statement#getMaxFieldSize()
     */
    public int getMaxFieldSize() throws SQLException {
        //no limit is zero according to the spec
        return 0;
    }

    /**
     * Returns the max rows allowed
     *
     * @see java.sql.Statement#getMaxRows()
     */
    public int getMaxRows() throws SQLException {
        //no limit is zero
        return 0;
    }

    /**
     * No limit here.
     *
     * @see java.sql.Statement#getQueryTimeout()
     */
    public int getQueryTimeout() throws SQLException {
        //no limit is zero according to the spec
        return 0;
    }

    /**
     * This statement will always return a read only ResultSet
     *
     * @see java.sql.Statement#getResultSetConcurrency()
     */
    public int getResultSetConcurrency() throws SQLException {
        return ResultSet.CONCUR_READ_ONLY;
    }

    /* (non-Javadoc)
     * @see java.sql.Statement#getResultSetHoldability()
     */
    public int getResultSetHoldability() throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    /* (non-Javadoc)
     * @see java.sql.Statement#getResultSetType()
     */
    public int getResultSetType() throws SQLException {
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    /* (non-Javadoc)
     * @see java.sql.Statement#getUpdateCount()
     */
    public int getUpdateCount() throws SQLException {
        return updateCount;
    }

    /* (non-Javadoc)
     * @see java.sql.Statement#cancel()
     */
    public void cancel() throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    /* (non-Javadoc)
     * @see java.sql.Statement#clearBatch()
     */
    public void clearBatch() throws SQLException {
        if (_queriesBatch != null)
            _queriesBatch.clear();
    }

    /* (non-Javadoc)
     * @see java.sql.Statement#clearWarnings()
     */
    public void clearWarnings() throws SQLException {
        // do nothing, we don't keep warnings anyways.
    }

    /**
     * This statement is not connected to the QueryProcessor, only the GConnection is. so this call
     * is silently ignored
     *
     * @see java.sql.Statement#close()
     */
    public void close() throws SQLException {
        if (resultSet != null)
            resultSet.close();
    }

    /* (non-Javadoc)
     * @see java.sql.Statement#getMoreResults()
     */
    public boolean getMoreResults() throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    /* (non-Javadoc)
     * @see java.sql.Statement#executeBatch()
     */
    public int[] executeBatch() throws SQLException {
        if (_queriesBatch != null && _queriesBatch.size() > 0) {
            boolean exceptionOccurred = false;
            String exceptionText = null;

            int[] result = new int[_queriesBatch.size()];
            for (int i = 0; i < _queriesBatch.size(); i++) {
                try {
                    result[i] = executeUpdate(_queriesBatch.get(i));
                } catch (SQLException e) {
                    exceptionOccurred = true;
                    exceptionText = e.getMessage();
                    result[i] = Statement.EXECUTE_FAILED;
                }
            }
            _queriesBatch.clear();
            if (exceptionOccurred)
                throw new BatchUpdateException(exceptionText, result);
            return result;
        }
        return new int[0];
    }

    /* (non-Javadoc)
     * @see java.sql.Statement#setFetchDirection(int)
     */
    public void setFetchDirection(int direction) throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    /* (non-Javadoc)
     * @see java.sql.Statement#setFetchSize(int)
     */
    public void setFetchSize(int rows) throws SQLException {
        handleUnsupportedSqlOperationsCalls("setFetchSize");
    }

    /**
     * Currently there shouldn't be any size limit.
     *
     * @see java.sql.Statement#setMaxFieldSize(int)
     */
    public void setMaxFieldSize(int max) throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    /**
     * Currently there shouldn't be any limit.
     *
     * @see java.sql.Statement#setMaxRows(int)
     */
    public void setMaxRows(int max) throws SQLException {
        handleUnsupportedSqlOperationsCalls("setMaxRows");
    }

    /**
     * Timeout is ignored.
     *
     * @see java.sql.Statement#setQueryTimeout(int)
     */
    public void setQueryTimeout(int seconds) throws SQLException {
        // JPA vendors are using this method so throwing an unsupported
        // exception in unacceptable.

    }

    /* (non-Javadoc)
     * @see java.sql.Statement#getMoreResults(int)
     */
    public boolean getMoreResults(int current) throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    /*
     * @see java.sql.Statement#setEscapeProcessing(boolean)
     */
    public void setEscapeProcessing(boolean enable) throws SQLException {
        //silently ignore

    }

    /**
     * Execute non-select queries
     *
     * @see java.sql.Statement#executeUpdate(java.lang.String)
     */
    public int executeUpdate(String sql) throws SQLException {
        init();

        if (sql.trim().toUpperCase().startsWith("SELECT"))
            throw new SQLException("Cannot call SELECT with executeUpdate. Use executeQuery instead", "GSP", -143);


        //otherwise we continue
        ResponsePacket response = connection.sendStatement(sql);
        //after the statement was sent and checked, we can return the result
        updateCount = response.getIntResult();
        return updateCount;
    }

    /* (non-Javadoc)
     * @see java.sql.Statement#addBatch(java.lang.String)
     */
    public void addBatch(String sql) throws SQLException {
        if (_queriesBatch == null)
            _queriesBatch = new ArrayList<String>();
        _queriesBatch.add(sql);
    }

    /* (non-Javadoc)
     * @see java.sql.Statement#setCursorName(java.lang.String)
     */
    public void setCursorName(String name) throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    /* (non-Javadoc)
     * @see java.sql.Statement#execute(java.lang.String)
     */
    public boolean execute(String sql) throws SQLException {
        init();

        ResponsePacket packet = connection.sendStatement(sql);
        if (packet.getResultEntry() != null) {
            buildResultSet(packet);
            return true;
        } else {
            updateCount = packet.getIntResult();
            return false;
        }
    }

    /**
     * Reset statement state
     */
    private void init() {
        updateCount = -1;
        resultSet = null;
    }

    /* (non-Javadoc)
     * @see java.sql.Statement#executeUpdate(java.lang.String, int)
	 */
    public int executeUpdate(String sql, int autoGeneratedKeys)
            throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    /* (non-Javadoc)
     * @see java.sql.Statement#execute(java.lang.String, int)
     */
    public boolean execute(String sql, int autoGeneratedKeys)
            throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    /* (non-Javadoc)
     * @see java.sql.Statement#executeUpdate(java.lang.String, int[])
     */
    public int executeUpdate(String sql, int[] columnIndexes)
            throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    /* (non-Javadoc)
     * @see java.sql.Statement#execute(java.lang.String, int[])
     */
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    /* (non-Javadoc)
     * @see java.sql.Statement#getConnection()
     */
    public Connection getConnection() throws SQLException {
        return connection;
    }

    /* (non-Javadoc)
     * @see java.sql.Statement#getGeneratedKeys()
     */
    public ResultSet getGeneratedKeys() throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    /* (non-Javadoc)
     * @see java.sql.Statement#getResultSet()
     */
    public ResultSet getResultSet() throws SQLException {
        return resultSet;
    }

    /**
     * No warnings are kept in this implementation which means it will always return null.
     *
     * @see java.sql.Statement#getWarnings()
     */
    public SQLWarning getWarnings() throws SQLException {
        //we don't keep any warnings.
        return null;
    }

    /* (non-Javadoc)
     * @see java.sql.Statement#executeUpdate(java.lang.String, java.lang.String[])
     */
    public int executeUpdate(String sql, String[] columnNames)
            throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    /* (non-Javadoc)
     * @see java.sql.Statement#execute(java.lang.String, java.lang.String[])
     */
    public boolean execute(String sql, String[] columnNames)
            throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    /* (non-Javadoc)
     * @see java.sql.Statement#executeQuery(java.lang.String)
     */
    public ResultSet executeQuery(String sql) throws SQLException {
        init();

        if (!connection.useNewDriver()) {
            if (!sql.trim().toUpperCase().startsWith("SELECT") &&
                    !sql.toUpperCase().startsWith("CALL"))
                throw new SQLException("Cannot call anything but SELECT with executeQuery. Use executeUpdate instead",
                        "GSP", -143);
        }

        ResponsePacket response = connection.sendStatement(sql);
        //query was sent and checked
        buildResultSet(response); //build the ResultSet
        return resultSet;
    }

    //translate the result entry to a GResultSet
    protected void buildResultSet(ResponsePacket response) {
        ResultEntry entry = response.getResultEntry();
        if( response instanceof ExplainPlanResponsePacket ){
            resultSet = new ExplainPlanGResultSet(this, entry,
                        ((ExplainPlanResponsePacket)response).getExplainPlan() );
        }
        else {
            resultSet = new GResultSet(this, entry);
        }
    }

    public boolean isClosed() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean isPoolable() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setPoolable(boolean poolable) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void closeOnCompletion() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean isCloseOnCompletion() throws SQLException {
        throw new UnsupportedOperationException();
    }

    private void handleUnsupportedSqlOperationsCalls(String operation) throws SQLException {

        if(!ignoreUnsupportedOptions){
            throw new SQLException("Command not Supported!", "GSP", -132);
        }

        if(_logger.isWarnEnabled()){
            _logger.warn("An unsupported java.sql.Statement." + operation + " command was called and ignored ");
        }

        return;
    }
}
