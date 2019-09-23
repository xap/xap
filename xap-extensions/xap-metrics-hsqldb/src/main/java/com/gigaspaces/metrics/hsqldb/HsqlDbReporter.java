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

package com.gigaspaces.metrics.hsqldb;

import com.gigaspaces.internal.utils.Singletons;
import com.gigaspaces.metrics.MetricRegistrySnapshot;
import com.gigaspaces.metrics.MetricReporter;
import com.gigaspaces.metrics.MetricTagsSnapshot;
import com.j_spaces.kernel.SystemProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.*;

import static com.gigaspaces.metrics.hsqldb.HsqlDBMetricsUtils.*;

/**
 * @author Evgeny
 * @since 15.0
 */
public class HsqlDbReporter extends MetricReporter {

    private static final Logger _logger = LoggerFactory.getLogger(HsqlDbReporter.class);

    private final HsqlDBReporterFactory factory;
    private final String url;
    private Connection connection;
    private final Map<String,PreparedStatement> _preparedStatements = new HashMap<>();
    private final static String hsqldDbConnectionKey = "HSQL_DB_CONNECTION";

    private static final Object _lock = new Object();

    private final Set<String> recordedMetricsTablesSet =
        new HashSet<>( Arrays.asList( METRICS_TABLES ) );

    //by default false
    private final boolean isAllMetricsRecordedToHsqlDb =
                        Boolean.getBoolean( SystemProperties.RECORDING_OF_ALL_METRICS_TO_HSQLDB_ENABLED);


    public HsqlDbReporter(HsqlDBReporterFactory factory) {
        super(factory);

        this.factory = factory;
        this.url = factory.getConnectionUrl();
        try {
            Class.forName(factory.getDriverClassName());
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Failed to load driver class " + factory.getDriverClassName(), e);
        }

        if (getOrCreateConnection() == null) {
            _logger.warn("Connection is not available yet - will try to reconnect on first report");
        }
    }

    private Connection getOrCreateConnection() {
        if (connection != null)
            return connection;
        try {
            synchronized (_lock) {
                if (connection != null)
                    return connection;
                connection = (Connection) Singletons.get(hsqldDbConnectionKey);
                if (connection != null)
                    return connection;
                _logger.debug("Connecting to [{}]", url);
                Connection newConnection = DriverManager.getConnection(url, factory.getUsername(), factory.getPassword());
                connection = (Connection) Singletons.putIfAbsent(hsqldDbConnectionKey, newConnection);
                if (connection != newConnection) {
                    _logger.debug("Closing temp connection");
                    tryClose(newConnection);
                }
                _logger.info("Connected to [{}]", url);
                if (_logger.isDebugEnabled()) {
                    _logger.debug(retrieveExistingTablesInfo(connection));
                }
                return connection;
            }
        } catch (SQLException e) {
            _logger.warn("Failed to connect to [{}]", url, e);
            return null;
        }
    }

    private static String retrieveExistingTablesInfo(Connection con) throws SQLException {
        StringBuilder result = new StringBuilder("! Existing public tables are:");
        try (ResultSet rs = con.getMetaData().getTables(con.getCatalog(), "PUBLIC", "%", null)) {
            while (rs.next()) {
                result.append(System.lineSeparator());
                result.append(rs.getString("TABLE_SCHEM"));
                result.append(".");
                result.append(rs.getString("TABLE_NAME"));
            }
        }
        return result.toString();
    }

    @Override
    protected void report(MetricRegistrySnapshot snapshot, MetricTagsSnapshot tags, String key, Object value) {
        Connection con = getOrCreateConnection();
        if (con == null) {
            _logger.warn("Report skipped - connection is not available yet [timestamp={}, key={}]", snapshot.getTimestamp(), key);
            return;
        }

        _logger.debug("Report, con={}, key={}", con, key);

        if( isAllMetricsRecordedToHsqlDb || recordedMetricsTablesSet.contains( key ) ) {

            final String realDbTableName = createValidTableName(key );

            Set<Map.Entry<String, Object>> tagEntries = tags.getTags().entrySet();
            StringBuilder columns = new StringBuilder();
            StringBuilder values = new StringBuilder();
            Map<Integer, Object> insertRowQueryValues = new HashMap<>();

            columns.append("TIME");
            columns.append(',');

            values.append('?');
            values.append(',');

            insertRowQueryValues.put(1, new Timestamp(snapshot.getTimestamp()));

            int index = 2;//it's 2 since TIME as a first parameter just added
            for (Map.Entry<String, Object> entry : tagEntries) {
                String entryKey = entry.getKey();
                Object entryValue = entry.getValue();

                _logger.debug("KEY={}, value class name: {}" + ", value={}",
                        entryKey, entryValue.getClass().getName(), entryValue);

                columns.append(entryKey);
                columns.append(',');

                values.append('?');//transformValue( entryValue ) );
                values.append(',');

                insertRowQueryValues.put(index, entryValue);

                index++;
            }

            columns.append("VALUE");
            values.append('?');
            insertRowQueryValues.put(index, value);

            final String
                insertSQL =
                "INSERT INTO " + realDbTableName + " (" + columns + ") VALUES (" + values + ")";

            try {
                PreparedStatement insertQueryPreparedStatement = _preparedStatements.get(insertSQL);
                _logger.debug("insert row query before setting values [{}], statement: {}", insertSQL, insertQueryPreparedStatement);

                if (insertQueryPreparedStatement == null) {
                    insertQueryPreparedStatement = con.prepareStatement(insertSQL);
                    //cache just created PreparedStatement if was not stored before
                    _preparedStatements.put(insertSQL, insertQueryPreparedStatement);
                }

                Set<Map.Entry<Integer, Object>>
                    insertRowQueryEntries =
                    insertRowQueryValues.entrySet();
                for (Map.Entry<Integer, Object> entry : insertRowQueryEntries) {
                    Integer paramIndex = entry.getKey();
                    Object paramValue = entry.getValue();
                    populatePreparedStatementWithParameters(insertQueryPreparedStatement,
                                                            paramIndex, paramValue);
                }
                _logger.trace("Before insert [{}]", insertSQL);

                insertQueryPreparedStatement.executeUpdate();

                _logger.trace("After insert [{}]", insertSQL);
            } catch (SQLSyntaxErrorException sqlSyntaxErrorException) {
                String message = sqlSyntaxErrorException.getMessage();
                _logger.debug("Report to {} failed: {}", realDbTableName, message);
                //if such table does not exist
                if (message != null && message.contains(
                    "user lacks privilege or object not found: " + realDbTableName)) {
                    //create such (not found ) table
                    String tableColumnsInfo = createTableColumnsInfo(tags, value);

                    try {
                        createTable(con, realDbTableName, tableColumnsInfo);
                    } catch (SQLException e) {
                        _logger.warn("Create table {} failed", realDbTableName, e);
                        //probably create table failed since table was just created, then try to
                        //call to this report method again, TODO: prevent loop
                        //report( snapshot, tags, key, value);
                    }
                }
                //any column does not exist
                else if (message != null &&
                         message.contains("user lacks privilege or object not found: ")) {

                    try {
                        handleAddingMissingTableColumns(con, tags, value, realDbTableName);
                    } catch (SQLException e) {
                        _logger.error("Failed to add missing columns to table [{}]", realDbTableName, e);
                    }
                }
            } catch (SQLException e) {
                _logger.error("Failed to insert row [{}]", insertSQL, e);
            }
        }
    }

    private void handleAddingMissingTableColumns( Connection con, MetricTagsSnapshot tags, Object value, String realDbTableName )
        throws SQLException {

            DatabaseMetaData dbm = con.getMetaData();
            ResultSet columnsResultSet = dbm.getColumns(null, null, realDbTableName, null);
            _logger.debug("Existing table {}, columns:", realDbTableName);
            List<String> existingTableColumns = new ArrayList<>();
            try {
                while( columnsResultSet.next() ){
                    String columnName = null;
                    try {
                        columnName = columnsResultSet.getString("COLUMN_NAME");
                        _logger.debug("col name={}, col type={}", columnName, columnsResultSet.getString("TYPE_NAME"));
                        existingTableColumns.add( columnName.toUpperCase() );
                    } catch (SQLException e) {
                        _logger.error("Failed to get column information", e);
                    }
                }
            } catch (SQLException e) {
                _logger.error("Failed to get column information", e);
            }

            Map<String, String> newTableColumnsMap = createTableColumnsMap(tags, value);
            Set<Map.Entry<String, String>> newTableColumnsEntries = newTableColumnsMap.entrySet();

            if (_logger.isDebugEnabled())
                _logger.debug("Passed for create table parameters: {}", Arrays.toString(newTableColumnsEntries.toArray(new Map.Entry[0])));

            //String prevColumnName = null;
            for( Map.Entry<String, String> newTableColumnEntry : newTableColumnsEntries ){
                String columnName = newTableColumnEntry.getKey().toUpperCase();
                //check if already exists in existing table
                if( !existingTableColumns.contains( columnName ) ){
                    String columnType = newTableColumnEntry.getValue();
                    _logger.debug("Before adding new column [{} {}] to table {} ", columnName, columnType, realDbTableName);
                    //String columnLocation = prevColumnName == null ? " FIRST " : " AFTER " + prevColumnName;
                    String addColumnQuery = "ALTER TABLE " + realDbTableName + " ADD " + columnName + " " + columnType; /*+ columnLocation*/
                    _logger.debug("addColumnQuery={}", addColumnQuery);
                    Statement statement = null;
                    try {
                        statement = con.createStatement();
                        statement.executeUpdate(addColumnQuery);
                        _logger.debug("Added new column [{} {}] to table {}", columnName, columnType, realDbTableName);
                        //createIndexOnTableAfterAddingColumn( statement, realDbTableName, columnName );
                    }
                    catch( SQLSyntaxErrorException sqlExc){
                        String exceptionMessage = sqlExc.getMessage();
                        //since sometimes at teh same times can be fet attempts to add the same column to the same table
                        if (exceptionMessage == null ||
                            !exceptionMessage.contains("object name already exists in statement" ) ) {
                            _logger.error("Failed to execute add column query", sqlExc);
                        }
                    }
                    catch (SQLException e1) {
                        _logger.error("Failed to execute add column query", e1);
                    }
                    finally{
                        if( statement != null ){
                            try {
                                statement.close();
                            }
                            catch (SQLException e){
                                _logger.warn("Failed to close statement", e );
                            }
                        }
                    }
                }
            }
    }

    private String createTableColumnsInfo( MetricTagsSnapshot tags, Object value) {

        Set<Map.Entry<String, Object>> entries = tags.getTags().entrySet();

        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append( '(' );
        strBuilder.append( "TIME" );
        strBuilder.append( ' ' );
        strBuilder.append( "TIMESTAMP" );
        strBuilder.append( ',' );

        for( Map.Entry<String, Object> entry : entries ){
            String columnName = entry.getKey();
            Object columnValue = entry.getValue();

            strBuilder.append( columnName );
            strBuilder.append( ' ' );
            strBuilder.append( getHSQLDBDataType( columnName, columnValue ) );
            strBuilder.append( ',' );
        }

        strBuilder.append( "VALUE" );
        strBuilder.append( ' ' );
        strBuilder.append( getHSQLDBDataType( "VALUE", value ) );
        strBuilder.append( ')' );

        _logger.debug("tables columns info={}", strBuilder);

        return strBuilder.toString();
    }

    //return map where key is column name, value is type
    private Map<String,String> createTableColumnsMap( MetricTagsSnapshot tags, Object value) {

        Set<Map.Entry<String, Object>> entries = tags.getTags().entrySet();
        //save insertion order
        Map<String,String> retColumnsMap = new LinkedHashMap<>(entries.size() );

        retColumnsMap.put( "TIME", "TIMESTAMP" );

        for( Map.Entry<String, Object> entry : entries ){
            String columnName = entry.getKey();
            Object columnValue = entry.getValue();
            retColumnsMap.put( columnName, getHSQLDBDataType( columnName, columnValue ) );
        }

        retColumnsMap.put( "VALUE", getHSQLDBDataType( "VALUE", value ) );

        if (_logger.isDebugEnabled())
            _logger.debug("retColumnsMap={}", Arrays.toString(retColumnsMap.entrySet().toArray(new Map.Entry[0])));

        return retColumnsMap;
    }

    private void populatePreparedStatementWithParameters(
        PreparedStatement insertQueryPreparedStatement, Integer paramIndex, Object paramValue) throws SQLException{

        if( paramValue instanceof String ) {
            insertQueryPreparedStatement.setString( paramIndex, paramValue.toString() );
        }
        else if( paramValue instanceof Timestamp ){
            insertQueryPreparedStatement.setTimestamp( paramIndex, ( Timestamp )paramValue );
        }
        else if( paramValue instanceof Integer ){
            insertQueryPreparedStatement.setInt( paramIndex, ( Integer )paramValue );
        }
        else if( paramValue instanceof Long ){
            insertQueryPreparedStatement.setLong( paramIndex, ( Long )paramValue );
        }
        else if( paramValue instanceof Double ){
            insertQueryPreparedStatement.setDouble( paramIndex, ( Double )paramValue );
        }
        else if( paramValue instanceof Float ){
            insertQueryPreparedStatement.setDouble( paramIndex, ( ( Float )paramValue ).doubleValue() ); ;
        }
        else if( paramValue instanceof Boolean ){
            insertQueryPreparedStatement.setBoolean( paramIndex, ( Boolean )paramValue );
        }
        else{
            _logger.warn("Value [{}] of class [{}] with index [{}] was not set", paramValue, paramValue.getClass().getName(), paramIndex);
        }
    }

    private String getHSQLDBDataType( String name, Object value ){

        String lowerCaseName = name.toLowerCase();
        if( value instanceof String ){
            String type;
            switch ( lowerCaseName){
                case "pid":
                    type = "VARCHAR(10)";
                    break;

                case "process_name":
                    type = "VARCHAR(10)";
                    break;

                case "ip":
                    type = "VARCHAR(15)";
                    break;

                case "pu_instance_id":
                    type = "VARCHAR(10)";
                    break;

                case "space_instance_id":
                    type = "VARCHAR(8)";
                    break;
                //in the case of ec2 instance host name can be long, like:
                //ip-xxx-xxx-xxx-xxx.eu-west-1.compute.internal
                case "host":
                    type = "VARCHAR(80)";
                    break;

                default:
                    type = "VARCHAR(40)";
            }

            return type;
        }
        if( value instanceof Timestamp ){
            return "TIMESTAMP";
        }
        if( value instanceof Boolean ){
            return "BOOLEAN";
        }
        if( value instanceof Number ){
            if( value instanceof Long ){
                return "BIGINT";
            }
            if( value instanceof Integer ){
                return "INTEGER";
            }
            if( value instanceof Short ){
                return "SMALLINT";
            }
            if( value instanceof Double ){
                return "REAL";
            }
            if( value instanceof Float ){
                return "REAL";
            }

            return "NUMERIC";
        }

        return "VARCHAR(40)";
    }

    @Override
    public void close() {
        if (connection != null) {
            tryClose(connection);
        }
        super.close();
    }

    private static void tryClose(Connection connection) {
        try {
            connection.close();
        } catch (SQLException e) {
            _logger.warn("Failed to close connection", e);
        }
    }

    private void createTable( Connection con, String tableName, String columnsInfo ) throws SQLException {
        Statement statement = null;
        try {
            statement = con.createStatement();

            final String sql = "CREATE CACHED TABLE " + tableName + " " + columnsInfo;

            _logger.debug("Create [{}] with sql query [{}]", tableName, sql);
            statement.executeUpdate(sql);
            _logger.debug("Table [{}] successfully created", tableName);
            createIndexOnTable( statement, tableName );
        }
        finally{
            if( statement != null ){
                try {
                    statement.close();
                }
                catch( SQLException e ){
                    _logger.warn("Failed to close statement", e );
                }
            }
        }
    }

    private void createIndexOnTable(Statement statement, String tableName) throws SQLException {
        String sql = "CREATE INDEX gsindex_" + tableName + " ON " + tableName + " ( TIME ASC )";//SPACE_ACTIVE
/*
        if( tableName.equals( TABLE_NAME_JVM_MEMORY_HEAP_USED_BYTES ) ||
            tableName.equals( TABLE_NAME_JVM_MEMORY_HEAP_USED_PERCENT ) ||
            tableName.equals(TABLE_NAME_PROCESS_CPU_USED_PERCENT ) ||
            tableName.equals( TABLE_NAME_SPACE_REPLICATION_REDO_LOG_USED_PERCENT ) ){
            sql = "CREATE INDEX gsindex_" + System.currentTimeMillis() + " ON " + tableName + " ( TIME ASC )";//PU_INSTANCE_ID
        }
        //space operations throughput
        else if( tableName.equals( TABLE_NAME_SPACE_OPERATIONS_EXECUTE_TP ) ||
                 tableName.equals( TABLE_NAME_SPACE_OPERATIONS_READ_MULTIPLE_TP ) ||
                 tableName.equals( TABLE_NAME_SPACE_OPERATIONS_READ_TP ) ||
                 tableName.equals( TABLE_NAME_SPACE_OPERATIONS_TAKE_TP ) ||
                 tableName.equals( TABLE_NAME_SPACE_OPERATIONS_TAKE_MULTIPLE_TP ) ||
                 tableName.equals( TABLE_NAME_SPACE_OPERATIONS_WRITE_TP ) ){
            sql = "CREATE INDEX gsindex_" + System.currentTimeMillis() + " ON " + tableName + " ( TIME ASC )";//SPACE_ACTIVE
        }*/
//        if( sql != null ){
        _logger.debug("Creating index for table [{}] by executing [{}]", tableName, sql);
        statement.executeUpdate(sql);
        _logger.debug("Index successfully created");
/*
        }
        else{
            _logger.info( "Index was not created for table [" + tableName + "]" );
        }
*/
    }
/*
    private void createIndexOnTableAfterAddingColumn(Statement statement, String tableName, String columnName ) throws SQLException {
        String sql = null;

        if( ( tableName.equals( TABLE_NAME_JVM_MEMORY_HEAP_USED_BYTES ) ||
            tableName.equals( TABLE_NAME_JVM_MEMORY_HEAP_USED_PERCENT ) ||
            tableName.equals(TABLE_NAME_PROCESS_CPU_USED_PERCENT ) ) &&
            columnName.equals( "PU_INSTANCE_ID" ) ){
            sql = "CREATE INDEX gsindex_" + System.currentTimeMillis() + " ON " + tableName + " ( PU_INSTANCE_ID )";//PU_INSTANCE_ID
        }
        if( sql != null ){
            _logger.info( "Creating index for table [" + tableName + "] by executing [" + sql + "]" );
            statement.executeUpdate(sql);
            _logger.info( "Index successfully created" );
        }
        else{
            _logger.info( "Index was not created for table [" + tableName + "]" );
        }
    }*/
}