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
package com.gigaspaces.jdbc;

import com.gigaspaces.jdbc.request.RequestPacketV3;
import com.j_spaces.core.IJSpace;
import com.j_spaces.jdbc.RequestPacket;
import com.j_spaces.jdbc.ResponsePacket;
import com.j_spaces.jdbc.driver.GConnection;
import com.j_spaces.jdbc.driver.GPreparedStatement;
import net.jini.core.transaction.Transaction;

import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GSConnection extends GConnection {
    private static final String DRIVER_VERSION = "v3";
    /*
    [^\\:]+ -> all chars till ':'
    (:\\d{4,5})? -> optional ':' followed by 4 or 5 digits
     */
    private static final String HOST_REGEX = "[^\\:]+(:\\d{4,5})?";
    private static final String MULTIPLE_HOST_REGEX = String.format("(%s(;%s)*)", HOST_REGEX, HOST_REGEX);
//    private static final String OPTIONAL_VERSION_REGEX = "((?<version>v3):)?";
    private static final String REQUIRED_VERSION_REGEX = "((?<version>v3):)";

    static final String CONNECTION_STRING_REGEX = String.format("jdbc:gigaspaces:%s//(?<host>%s)/(?<spaceName>.*)", REQUIRED_VERSION_REGEX, MULTIPLE_HOST_REGEX);

    private static final String CONNECTION_TEMPLATE = String.format("jdbc:gigaspaces:%s://hostname[:port]/spaceName", DRIVER_VERSION);

    GSConnection(String url, Properties properties) {
        super(url, properties);
    }

    public GSConnection(IJSpace space, Properties properties) throws SQLException {
        super(space, properties);
    }

    public static GSConnection getInstance(IJSpace space, Properties properties)
            throws SQLException {
        return new GSConnection(space, properties);
    }


    @Override
    protected String validateAndGetSpaceUrl(String connectionString) throws SQLException {
        Pattern pattern = Pattern.compile(CONNECTION_STRING_REGEX);
        Matcher matcher = pattern.matcher(connectionString);
        if (matcher.matches()) {
            String version = matcher.group("version");
            if (version != null && !version.equals("v3"))
                throw new SQLException("Unmatched version [" + version + "] for driver");
            String hosts = String.join(",", matcher.group("host").split(";"));
            String spaceName = matcher.group("spaceName");
            return "jini://*/*/" + spaceName + "?locators=" + hosts;
        } else {
            throw new SQLException("Invalid Url [" + connectionString + "] - does not match " + CONNECTION_TEMPLATE);
        }
    }

    @Override
    public ResponsePacket sendStatement(String statement) throws SQLException {
        RequestPacket packet = new RequestPacketV3();
        packet.setType(RequestPacket.Type.STATEMENT);
        packet.setStatement(statement);
        return writeRequestPacket(packet);
    }

    @Override
    public ResponsePacket sendPreparedStatement(String statement, Object[] values) throws SQLException {
        RequestPacket packet = new RequestPacketV3();
        packet.setType(RequestPacket.Type.PREPARED_WITH_VALUES);
        packet.setStatement(statement);
        packet.setPreparedValues(values);
        return writeRequestPacket(packet);
    }

    @Override
    public boolean useNewDriver() {
        return true;
    }


    //////
    @Override
    public void setTransaction(Transaction transaction){
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public ResultSet getTableColumnsInformation(String tableName){
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public int getHoldability(){
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public int getTransactionIsolation(){
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void clearWarnings(){
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void commit(){
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void rollback(){
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public boolean getAutoCommit(){
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public boolean isClosed() throws SQLException {
        return super.isClosed();
    }

    @Override
    public boolean isReadOnly(){
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void setHoldability(int holdability){
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void setTransactionIsolation(int level){
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void setAutoCommit(boolean autoCommit){
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void setUseSingleSpace(boolean useSingleSpace){
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void setReadOnly(boolean readOnly){
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public String getCatalog(){
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void setCatalog(String catalog){
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public DatabaseMetaData getMetaData(){
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public SQLWarning getWarnings(){
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public Savepoint setSavepoint(){
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint){
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void rollback(Savepoint savepoint){
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public Statement createStatement() throws SQLException {
        return super.createStatement();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency){
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability){
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public Map<String, Class<?>> getTypeMap(){
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map){
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public String nativeSQL(String sql){
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public CallableStatement prepareCall(String sql){
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency){
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability){
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        GPreparedStatement ps = new GPreparedStatement(this, sql);
        return ps;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys){
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency){
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability){
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes){
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public Savepoint setSavepoint(String name){
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames){
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements){
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public java.sql.Blob createBlob(){
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public java.sql.Clob createClob(){
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes){
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public Properties getClientInfo(){
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public String getClientInfo(String name){
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public boolean isValid(int timeout){
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface){
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public <T> T unwrap(Class<T> iface){
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public NClob createNClob(){
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public SQLXML createSQLXML(){
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void setClientInfo(String name, String value) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void setClientInfo(Properties properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void setSchema(String schema){
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public String getSchema(){
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void abort(Executor executor){
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds){
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public int getNetworkTimeout(){
        throw new UnsupportedOperationException("Unsupported");
    }
}
