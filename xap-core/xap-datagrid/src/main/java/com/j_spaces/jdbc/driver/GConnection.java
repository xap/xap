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

import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.gigaspaces.internal.client.spaceproxy.router.SpaceProxyRouter;
import com.gigaspaces.internal.server.space.IRemoteSpace;
import com.gigaspaces.jdbc.request.RequestPacketV3;
import com.gigaspaces.security.directory.DefaultCredentialsProvider;
import com.j_spaces.core.IJSpace;
import com.j_spaces.core.admin.IRemoteJSpaceAdmin;
import com.j_spaces.core.client.BasicTypeInfo;
import com.j_spaces.core.client.SpaceFinder;
import com.j_spaces.jdbc.*;
import com.j_spaces.jdbc.batching.BatchResponsePacket;
import com.j_spaces.jdbc.driver.GPreparedStatement.PreparedValuesCollection;
import com.j_spaces.jdbc.request.SetAutoCommitRequest;
import com.j_spaces.jdbc.request.SetTransaction;
import com.j_spaces.jdbc.request.SetUseSingleSpace;
import net.jini.core.transaction.Transaction;
import net.jini.core.transaction.server.ServerTransaction;

import java.io.IOException;
import java.rmi.RemoteException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

/**
 * The connection implementation that connects to the QueryProcessor on the GigaSpaces server side.
 * <B>Note:</B> This implementation is NOT thread-safe. Do not share instances between threads!
 *
 * @author Michael Mitrani, 2Train4
 * @author yuval mishory
 */
@com.gigaspaces.api.InternalApi
public class GConnection implements Connection {

    public static final String READ_MODIFIERS = "readModifiers";
    public static final String USE_NEW_DRIVER = "useNewDriver";
    public static final String JDBC_GIGASPACES = "jdbc:gigaspaces:";
    public static final String JDBC_GIGASPACES_URL = "jdbc:gigaspaces:url:";
    private static final long EXECUTE_RETRY_TIMEOUT = 60 * 1000l;

    private String url;
    private ISpaceProxy space;
    private IQueryProcessor qp;
    private ConnectionContext context;
    private Integer readModifiers;
    private Properties properties;
    private Boolean useNewDriver = false;

    private GConnection(IJSpace space, Properties properties) throws SQLException {
        try {
            this.url = JDBC_GIGASPACES_URL + space.getURL().getURL();
            this.space = (ISpaceProxy) space;
            this.properties = properties;
            initialize(space.getDirectProxy().getRemoteJSpace());
        } catch (Exception e) {
            SQLException se = new SQLException("Connect to space failed:[ " + space.getURL().getURL() + "]");
            se.initCause(e);
            throw se;
        }
    }

    /**
     * Create new connection from given url and properties
     */
    public GConnection(String url, Properties properties) throws SQLException {
        try {
            if (!url.startsWith(JDBC_GIGASPACES_URL))
                throw new IllegalArgumentException("Invalid Url [" + url + "] - does not start with " + JDBC_GIGASPACES_URL);

            this.url = url.substring(JDBC_GIGASPACES_URL.length());
            //noinspection deprecation
            this.space = (ISpaceProxy) SpaceFinder.find(this.url);
            this.properties = properties;
            initialize(space.getDirectProxy().getRemoteJSpace());
        } catch (Exception e) {
            SQLException se = new SQLException("Error creating connection; Cause: " + e, "GSP", -137);
            se.initCause(e);
            throw se;
        }
    }

    private void initialize(final IRemoteSpace remoteSpace) throws Exception {
        if (properties != null) {
            // NOTE: we explicitly use get() instead of getProperty() since the value might not be a string...
            Object modifiersProp = properties.get(READ_MODIFIERS);
            readModifiers = modifiersProp == null ? null : Integer.valueOf(modifiersProp.toString());

            Object useNewDriverProp = properties.get(USE_NEW_DRIVER);
            useNewDriver = useNewDriverProp == null ? false : Boolean.valueOf(useNewDriverProp.toString());

            String username = properties.getProperty(ConnectionContext.USER);
            String password = properties.getProperty(ConnectionContext.PASSWORD);
            if (username != null && username.length() > 0) {
                // TODO: Consider warning/skipping if already logged in.
                space.getDirectProxy().login(new DefaultCredentialsProvider(username, password));
            }
        }

        qp = QueryProcessorFactory.newInstance(space, remoteSpace, properties);
        context = qp.newConnection();
        context.setSpaceContext(space.getDirectProxy().getSecurityManager().acquireContext(remoteSpace));
    }

    public static GConnection getInstance(IJSpace space)
            throws SQLException {
        return new GConnection(space, new Properties());
    }

    public static GConnection getInstance(IJSpace space, Properties properties)
            throws SQLException {
        return new GConnection(space, properties);
    }

    public static Connection getInstance(IJSpace space, String username, String password)
            throws SQLException {
        Properties config = new Properties();
        config.setProperty(ConnectionContext.USER, username);
        config.setProperty(ConnectionContext.PASSWORD, password);
        return new GConnection(space, config);
    }

    /**
     * Sets the transaction the connection will use. Only relevant when auto commit is set to
     * false.
     *
     * @param transaction The transaction the connection will use.
     */
    public void setTransaction(Transaction transaction) throws SQLException {
        if (getAutoCommit())
            throw new IllegalArgumentException("Setting a transaction is not allowed when autoCommit = true.");
        try {
            if (transaction instanceof ServerTransaction) {
                ServerTransaction serverTransaction = (ServerTransaction) transaction;
                serverTransaction = serverTransaction.createCopy();
                serverTransaction.setEmbeddedMgrProxySideInstance(false);
                RequestPacket packet = new SetTransaction(serverTransaction);
                writeRequestPacket(packet);
            }
            qp.setTransaction(transaction);
        } catch (RemoteException e) {
            throw new SQLException(e.getMessage());
        }
    }

    /**
     * Returns a ResultSet containing columns information for the provided table name.
     *
     * @param tableName The table to get columns information for.
     * @return ResultSet containing columns informations.
     */
    public ResultSet getTableColumnsInformation(String tableName) throws SQLException {
        String[] columns = {"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME",
                "DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS",
                "NUM_PREC_RADIX", "NULLABLE", "REMARKS", "COLUMN_DEF", "SQL_DATA_TYPE",
                "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH", "ORDINAL_POSITION", "IS_NULLABLE",
                "SCOPE_CATALOG", "SCOPE_SCHEMA", "SCOPE_TABLE", "SOURCE_DATA_TYPE"};

        String[] tables = {"", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "",
                "", "", "", "", "", "", ""};

        ResultEntry result;
        ArrayList<Object[]> rows = new ArrayList<Object[]>();

        try {
            ISpaceProxy spaceProxy = space;
            IRemoteJSpaceAdmin admin = (IRemoteJSpaceAdmin) spaceProxy.getAdmin();

            //noinspection deprecation
            BasicTypeInfo typeInfo;
            try {
                // getClassTypeInfo causes a NullPointerException if the provided
                // type doesn't exist. this will be fixed in the future.
                //noinspection deprecation
                typeInfo = admin.getClassTypeInfo(tableName);
                if (typeInfo != null) {
                    for (int i = 0; i < typeInfo.m_FieldsNames.length; i++) {
                        Object[] row = new Object[23];
                        row[0] = null;
                        row[1] = null;
                        row[2] = tableName;
                        row[3] = typeInfo.m_FieldsNames[i];
                        row[4] = Types.OTHER;
                        row[5] = typeInfo.m_FieldsTypes[i];
                        row[6] = 0;
                        row[7] = 0;
                        row[8] = 0;
                        row[9] = 10;
                        row[10] = GDatabaseMetaData.columnNullableUnknown;
                        row[11] = null;
                        row[12] = null;
                        row[13] = 0;
                        row[14] = 0;
                        row[15] = 0;
                        row[16] = i + 1;
                        row[17] = "";
                        row[18] = null;
                        row[19] = null;
                        row[20] = null;
                        row[21] = null;
                        rows.add(row);
                    }
                }
            } catch (NullPointerException e) {
                // Type doesn't exist, continue.
            }

            Object[][] valuesArray = new Object[rows.size()][];
            for (int i = 0; i < rows.size(); i++) {
                valuesArray[i] = rows.get(i);
            }
            result = new ResultEntry(columns, columns, tables, valuesArray);

        } catch (RemoteException e) {
            SQLException sqe = new SQLException(e.getMessage());
            sqe.initCause(e);
            throw sqe;
        }
        return new GResultSet(null, result);
    }

    /* (non-Javadoc)
     * @see java.sql.Connection#getHoldability()
     */
    public int getHoldability() throws SQLException {
        throw new SQLException("Command not Supported!", "GSP", -132);
    }

    /* (non-Javadoc)
     * @see java.sql.Connection#getTransactionIsolation()
     */
    public int getTransactionIsolation() throws SQLException {
        return Connection.TRANSACTION_READ_UNCOMMITTED;
    }

    /* (non-Javadoc)
     * @see java.sql.Connection#clearWarnings()
     */
    public void clearWarnings() throws SQLException {
        // do nothing
    }

    /* (non-Javadoc)
     * @see java.sql.Connection#close()
     */
    public void close() throws SQLException {
        try {
            qp.closeConnection(context);

            qp = null; //help gc
        } catch (IOException ioe) {
            throw new SQLException(ioe.getMessage());
        }

    }

    /* (non-Javadoc)
     * @see java.sql.Connection#commit()
     */
    public void commit() throws SQLException {
        sendStatement("COMMIT");
    }

    /* (non-Javadoc)
     * @see java.sql.Connection#rollback()
     */
    public void rollback() throws SQLException {
        sendStatement("ROLLBACK");
    }

    /* (non-Javadoc)
     * @see java.sql.Connection#getAutoCommit()
     */
    public boolean getAutoCommit() throws SQLException {
        try {
            return qp.getSession(context).isAutoCommit();
        } catch (RemoteException e) {
            SQLException se = new SQLException("Error in calling getAutoCommit() on QueryProcessor. ");
            se.initCause(e);
            throw se;
        }
    }

    /* (non-Javadoc)
     * @see java.sql.Connection#isClosed()
     */
    public boolean isClosed() throws SQLException {
        try {
            return !qp.isAvailable();
        } catch (RemoteException e) {
            return true;
        }
    }

    /* (non-Javadoc)
     * @see java.sql.Connection#isReadOnly()
     */
    public boolean isReadOnly() throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.Connection#setHoldability(int)
     */
    public void setHoldability(int holdability) throws SQLException {
        throw new SQLException("Not Supported!");
    }

    /* (non-Javadoc)
     * @see java.sql.Connection#setTransactionIsolation(int)
     */
    public void setTransactionIsolation(int level) throws SQLException {
        if (level != Connection.TRANSACTION_READ_UNCOMMITTED)
            throw new SQLException("This isolation level is not supported");
    }

    /* (non-Javadoc)
     * @see java.sql.Connection#setAutoCommit(boolean)
     */
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        RequestPacket packet = new SetAutoCommitRequest(autoCommit);

        writeRequestPacket(packet);
    }

    /**
     * @param useSingleSpace to use single space
     */
    public void setUseSingleSpace(boolean useSingleSpace) throws SQLException {
        RequestPacket packet = new SetUseSingleSpace(useSingleSpace);

        writeRequestPacket(packet);
    }

    /* (non-Javadoc)
     * @see java.sql.Connection#setReadOnly(boolean)
     */
    public void setReadOnly(boolean readOnly) throws SQLException {
        //do nothing
    }

    /* (non-Javadoc)
     * @see java.sql.Connection#getCatalog()
     */
    public String getCatalog() throws SQLException {
        //not supported
        return null;
    }

    /* (non-Javadoc)
     * @see java.sql.Connection#setCatalog(java.lang.String)
     */
    public void setCatalog(String catalog) throws SQLException {
        //do nothing
    }

    /* (non-Javadoc)
     * @see java.sql.Connection#getMetaData()
     */
    public DatabaseMetaData getMetaData() throws SQLException {
        return new GDatabaseMetaData(this);
    }

    /* (non-Javadoc)
     * @see java.sql.Connection#getWarnings()
     */
    public SQLWarning getWarnings() throws SQLException {
        // return null, as if they were no warnings.
        return null;
    }

    /* (non-Javadoc)
     * @see java.sql.Connection#setSavepoint()
     */
    public Savepoint setSavepoint() throws SQLException {
        throw new SQLException("Not Supported!");
    }

    /* (non-Javadoc)
     * @see java.sql.Connection#releaseSavepoint(java.sql.Savepoint)
     */
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new SQLException("Not Supported!");
    }

    /* (non-Javadoc)
     * @see java.sql.Connection#rollback(java.sql.Savepoint)
     */
    public void rollback(Savepoint savepoint) throws SQLException {
        throw new SQLException("Not Supported!");
    }

    /* (non-Javadoc)
     * @see java.sql.Connection#createStatement()
     */
    public Statement createStatement() throws SQLException {
        return new GStatement(this);
    }

    /* (non-Javadoc)
     * @see java.sql.Connection#createStatement(int, int)
     */
    public Statement createStatement(int resultSetType, int resultSetConcurrency)
            throws SQLException {
        throw new SQLException("Not Supported!");
    }

    /* (non-Javadoc)
     * @see java.sql.Connection#createStatement(int, int, int)
     */
    public Statement createStatement(int resultSetType,
                                     int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        throw new SQLException("Not Supported!");
    }

    /* (non-Javadoc)
     * @see java.sql.Connection#getTypeMap()
     */
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throw new SQLException("Not Supported!");
    }

    /* (non-Javadoc)
     * @see java.sql.Connection#setTypeMap(java.util.Map)
     */
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw new SQLException("Not Supported!");
    }

    /* (non-Javadoc)
     * @see java.sql.Connection#nativeSQL(java.lang.String)
     */
    public String nativeSQL(String sql) throws SQLException {
        // return the given string, no client side parsing.
        return sql;
    }

    /* (non-Javadoc)
     * @see java.sql.Connection#prepareCall(java.lang.String)
     */
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new SQLException("Not Supported!");
    }

    /* (non-Javadoc)
     * @see java.sql.Connection#prepareCall(java.lang.String, int, int)
     */
    public CallableStatement prepareCall(String sql, int resultSetType,
                                         int resultSetConcurrency) throws SQLException {
        throw new SQLException("Not Supported!");
    }

    /* (non-Javadoc)
     * @see java.sql.Connection#prepareCall(java.lang.String, int, int, int)
     */
    public CallableStatement prepareCall(String sql, int resultSetType,
                                         int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        throw new SQLException("Not Supported!");
    }

    /* (non-Javadoc)
     * @see java.sql.Connection#prepareStatement(java.lang.String)
     */
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        //at this stage, we send the statement to the QueryProcessor
        //and return a new PreparedStatement.
        GPreparedStatement ps = new GPreparedStatement(this, sql);
        RequestPacket packet = new RequestPacket();
        packet.setType(RequestPacket.Type.PREPARED_STATEMENT);
        packet.setStatement(sql);
        packet.setModifiers(readModifiers);
        writeRequestPacket(packet);

        //if we got here, no exception was thrown.
        return ps;
    }

    /* (non-Javadoc)
     * @see java.sql.Connection#prepareStatement(java.lang.String, int)
     */
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
            throws SQLException {
        throw new SQLException("Not Supported!");
    }

    /* (non-Javadoc)
     * @see java.sql.Connection#prepareStatement(java.lang.String, int, int)
     */
    public PreparedStatement prepareStatement(String sql, int resultSetType,
                                              int resultSetConcurrency) throws SQLException {
        return prepareStatement(sql);
    }

    /* (non-Javadoc)
     * @see java.sql.Connection#prepareStatement(java.lang.String, int, int, int)
     */
    public PreparedStatement prepareStatement(String sql, int resultSetType,
                                              int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        throw new SQLException("Not Supported!");
    }

    /* (non-Javadoc)
     * @see java.sql.Connection#prepareStatement(java.lang.String, int[])
     */
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes)
            throws SQLException {
        throw new SQLException("Not Supported!");
    }

    /* (non-Javadoc)
     * @see java.sql.Connection#setSavepoint(java.lang.String)
     */
    public Savepoint setSavepoint(String name) throws SQLException {
        throw new SQLException("Not Supported!");
    }

    /* (non-Javadoc)
     * @see java.sql.Connection#prepareStatement(java.lang.String, java.lang.String[])
     */
    public PreparedStatement prepareStatement(String sql, String[] columnNames)
            throws SQLException {
        throw new SQLException("Not Supported!");
    }

    /**
     * This is the methods that writes the RequestPacket into the socket
     *
     * @param packet the RequestPacket to send
     * @return ResponsePacket The packet returned from the QueryProcessor
     */
    public ResponsePacket writeRequestPacket(RequestPacket packet) throws SQLException {
        long start = System.currentTimeMillis();
        boolean useRouter = false;
        while (true) {
            if (!useRouter) {
                try {
                    return writeRequestPacketInternal(packet);
                } catch (RemoteException re) {
                    // sleep for a bit, maybe the problem is temporary
                    sleep(1000);
                    if (System.currentTimeMillis() - start > EXECUTE_RETRY_TIMEOUT) {
                        // timeout: abandon current remote space, find another.
                        useRouter = true;
                    }
                }
            } else {
                reinitialize();
                // reset for next iteration
                start = System.currentTimeMillis();
                useRouter = false;
            }
        }
    }

    private void reinitialize() throws SQLException {
        // use router to find a new remote space
        long startDiscovery = System.currentTimeMillis();
        SpaceProxyRouter router = space.getDirectProxy().getProxyRouter();
        while (true) {
            // try and retry to reinitialize this, until timeout is reached.
            IRemoteSpace newRemoteSpace = router.getAnyActiveSpace();
            try {
                initialize(newRemoteSpace);
                break;
            } catch (Exception e) {
                // don't rush into another attempt
                sleep(1000);
                if (System.currentTimeMillis() - startDiscovery > EXECUTE_RETRY_TIMEOUT) {
                    // timeout: give up
                    throw new SQLException("Remote space is unreachable");
                }
            }
        }
    }

    private ResponsePacket writeRequestPacketInternal(RequestPacket packet) throws RemoteException, SQLException {
        ResponsePacket responsePacket;
        if (packet.getModifiers() != null)
            return qp.executeQuery(new ExtendedRequestPacket(packet), context);

        responsePacket = qp.executeQuery(packet, context);
        return responsePacket;
    }

    private void sleep(long l) {
        try {
            Thread.sleep(l);
        } catch (InterruptedException ignored) {
        }
    }

    public Blob createBlob(byte[] bytes) throws SQLException {
        return new Blob(bytes, this);
    }

    public Clob createClob(String clob) throws SQLException {
        return new Clob(clob, this);
    }

    /**
     * Send a packet that contains a statement.
     *
     * @param statement the sql statement that should be sent
     * @return The ResponsePacket received from the QueryProcessor
     */
    public ResponsePacket sendStatement(String statement) throws SQLException {
        RequestPacket packet = useNewDriver ? new RequestPacketV3() : new RequestPacket();
        packet.setModifiers(readModifiers);
        packet.setType(RequestPacket.Type.STATEMENT);
        packet.setStatement(statement);
        return writeRequestPacket(packet);
    }

    /**
     * Send a packet that contains a PreparedStatement.
     *
     * @param statement the SQL statement that should be sent.
     * @return The ResponsePacket received from the QueryProcessor
     */
    public ResponsePacket sendPreparedStatement(String statement, Object[] values) throws SQLException {
        RequestPacket packet = new RequestPacket();
        packet.setModifiers(readModifiers);
        packet.setType(RequestPacket.Type.PREPARED_WITH_VALUES);
        packet.setStatement(statement);
        packet.setPreparedValues(values);
        return writeRequestPacket(packet);
    }

    /**
     * Send a packet that contains a PreparedStatement values batch.
     */
    public BatchResponsePacket sendPreparedStatementBatch(String statement,
                                                          PreparedValuesCollection preparedValuesCollection) throws SQLException {
        RequestPacket packet = new RequestPacket();
        packet.setType(RequestPacket.Type.PREPARED_VALUES_BATCH);
        packet.setStatement(statement);
        packet.setModifiers(readModifiers);
        packet.setPreparedValuesCollection(preparedValuesCollection);
        return (BatchResponsePacket) writeRequestPacket(packet);
    }

    /**
     * @return the url of this connection
     */
    public String getUrl() {
        return url;
    }

    public Array createArrayOf(String typeName, Object[] elements)
            throws SQLException {
        throw new UnsupportedOperationException();
    }

    public java.sql.Blob createBlob() throws SQLException {
        return new Blob(new byte[0], this);
    }

    public java.sql.Clob createClob() throws SQLException {
        return new Clob("", this);
    }

    public Struct createStruct(String typeName, Object[] attributes)
            throws SQLException {
        throw new UnsupportedOperationException();
    }


    public Properties getClientInfo() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public String getClientInfo(String name) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean isValid(int timeout) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public NClob createNClob() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setClientInfo(String name, String value)
            throws SQLClientInfoException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setClientInfo(Properties properties)
            throws SQLClientInfoException {
        throw new UnsupportedOperationException();
    }

    public void setSchema(String schema) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public String getSchema() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void abort(Executor executor) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setNetworkTimeout(Executor executor, int milliseconds)
            throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int getNetworkTimeout() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Boolean useNewDriver() {
        return useNewDriver;
    }
}
