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

import com.gigaspaces.metrics.MetricRegistrySnapshot;
import com.gigaspaces.metrics.MetricReporter;
import com.gigaspaces.metrics.MetricTagsSnapshot;
import com.j_spaces.kernel.SystemProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;

/**
 * @author Evgeny
 * @since 15.0
 */
public class HsqlDbReporter extends MetricReporter {

    private static final Logger _logger = LoggerFactory.getLogger(HsqlDbReporter.class);
    private static final boolean systemFilterDisabled = Boolean.getBoolean(SystemProperties.RECORDING_OF_ALL_METRICS_TO_HSQLDB_ENABLED);

    private final SharedJdbcConnectionWrapper connectionWrapper;
    private final String dbTypeString;
    private final Map<String,PreparedStatement> _preparedStatements = new HashMap<>();
    private final Set<PreparedStatement> _statementsForBatch = new LinkedHashSet<>();

    private final static String EXCEPTION_MESSAGE_MISSING_OBJECT = "user lacks privilege or object not found: ";

    private final static Set<String> VM_TABLE_COLUMNS_WITH_POSSIBLE_NULL_VALUES =
                    new HashSet<>( Arrays.asList( TableColumnNames.PU_NAME_COLUMN_NAME.toUpperCase(),
                                TableColumnNames.PU_INSTANCE_ID_COLUMN_NAME.toUpperCase() ) );

    public HsqlDbReporter(HsqlDBReporterFactory factory, SharedJdbcConnectionWrapper connectionWrapper) {
        super(factory);
        this.connectionWrapper = connectionWrapper;
        this.dbTypeString = factory.getDbTypeString();
        try {
            Class.forName(factory.getDriverClassName());
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Failed to load driver class " + factory.getDriverClassName(), e);
        }
    }

    @Override
    public void close() {
        this.connectionWrapper.close();
        super.close();
    }


    @Override
    public void report(List<MetricRegistrySnapshot> snapshots) {
        Connection con = connectionWrapper.getOrCreateConnection();
        if (con == null) {
            if (!connectionWrapper.isSilent())
                _logger.warn("Report skipped - connection is not available yet");
            return;
        }

        super.report(snapshots);
    }

    @Override
    protected void report(MetricRegistrySnapshot snapshot, MetricTagsSnapshot tags, String key, Object value) {
        String tableName = getTableName(key);
        if (tableName == null) {
            _logger.debug("Report skipped - key was filtered out [timestamp={}, key={}]", snapshot.getTimestamp(), key);
            return;
        }
        Object spaceActiveTagValue = tags.getTags().get("space_active");
        //Fix for GS-14336, don't continue if space_active is false since we don't want to record such metric record
        //in hsqldb and don't want to display it in ui
        if( spaceActiveTagValue != null && !( Boolean )spaceActiveTagValue ){
            return;
        }

        Connection con = connectionWrapper.getOrCreateConnection();
        if (con == null) {
            if (!connectionWrapper.isSilent())
                _logger.warn("Report skipped - connection is not available yet [timestamp={}, key={}]", snapshot.getTimestamp(), key);
            return;
        }

        _logger.debug("Report, con={}, key={}", con, key);
        List<Object> values = new ArrayList<>();
        List<String> columns = new ArrayList<>();
        String insertSQL = "";
        try {
            insertSQL = generateInsertQuery(tableName, snapshot.getTimestamp(), value, tags, values,columns);

            PreparedStatement statement = getOrCreatePreparedStatement(insertSQL, con);
            for (int i=0 ; i < values.size() ; i++) {
                String columnName = i >= columns.size() ? null : columns.get( i );
                setParameter(statement, i + 1, values.get( i ), columnName );
            }
            _logger.trace("Before adding insert to batch [{}]", insertSQL);
            statement.addBatch();
            _logger.trace("After adding insert to batch [{}]", insertSQL);
            _statementsForBatch.add( statement );
        } catch (SQLSyntaxErrorException e) {
            String message = e.getMessage();
            _logger.debug("Report to {} failed: {}", tableName, message);
            if (message != null && message.contains(EXCEPTION_MESSAGE_MISSING_OBJECT + tableName)) {
                createTable(con, tableName, value, tags);
            } else if (message != null && message.contains(EXCEPTION_MESSAGE_MISSING_OBJECT)) {
                String missingColumnName = message.replace( EXCEPTION_MESSAGE_MISSING_OBJECT, "" ).trim();
                Map<String,Object> clonedTags = new HashMap<>( tags.getTags() );
                if( VM_TABLE_COLUMNS_WITH_POSSIBLE_NULL_VALUES.contains( missingColumnName ) ){
                    clonedTags.put( missingColumnName, null );
                }
                addMissingColumns(con, tableName, clonedTags);
            } else {
                _logger.warn("Failed to insert row [{}] using values [{}]" , insertSQL,
                              Arrays.toString(values.toArray(new Object[0])), e);
            }
        } catch (SQLTransientConnectionException | SQLNonTransientConnectionException e){
            _logger.warn("Failed to insert row [{}] using values [{}], resetting connection...", insertSQL,
                    Arrays.toString(values.toArray(new Object[0])), e);
           handleConnectionError(con);
        } catch (SQLException e) {
            //internal hsqldb exception, in later versions becomes General error
            if( e.toString().contains( "NullPointerException" ) ) {
                _logger.info("Failed to insert row [{}] using values [{}] due to SQLException", insertSQL,
                           Arrays.toString(values.toArray(new Object[0])) );
            }
            else {
                _logger
                    .warn("Failed to insert row [{}] using values [{}]", insertSQL,
                           Arrays.toString(values.toArray(new Object[0])), e);
            }
        }
        catch( Throwable t ){
            _logger
                    .warn("Failed to insert row [{}] using values [{}] to table [{}]", insertSQL,
                            Arrays.toString(values.toArray(new Object[0])), tableName, t);
        }
    }

    @Override
    protected void flush() {

        for(PreparedStatement statement : _statementsForBatch){
            try {
                statement.executeBatch();
            }
            catch (SQLTransientConnectionException | SQLNonTransientConnectionException e){
                _logger.warn( "Failed to insert row to table using statement [{}] while executing batch", statement, e);
                handleConnectionError( connectionWrapper.getOrCreateConnection() );
                return;
            }
            catch (SQLException sqlException) {
                _logger.warn("Failed to insert row to table using statement [{}]", statement, sqlException);
            }
        }

         _statementsForBatch.clear();
    }

    private void handleConnectionError(Connection connection) {
        _logger.info( "Reset connection to HSQLDB due to connection error" );
        connectionWrapper.resetConnection(connection);
        _preparedStatements.clear();
        _statementsForBatch.clear();
    }

    private PreparedStatement getOrCreatePreparedStatement(String sql, Connection connection) throws SQLException {
        PreparedStatement statement = _preparedStatements.get(sql);
        if (statement == null) {
            statement = connection.prepareStatement(sql);
            _preparedStatements.put(sql, statement);
        }
        return statement;
    }

    private String getTableName(String key) {
        SystemMetrics systemMetrics = SystemMetricsManager.getSystemMetric(key);
        if( systemMetrics != null ){
            return systemMetrics.getTableName();
        }
        return systemFilterDisabled ? PredefinedSystemMetrics.toTableName(key) : null;
    }

    private void setParameter(PreparedStatement statement, Integer index, Object value, String columnName ) throws SQLException {
        if (value instanceof String) {
            statement.setString(index, (String)value);
        } else if (value instanceof Timestamp) {
            statement.setTimestamp(index, (Timestamp)value);
        } else if (value instanceof Integer) {
            statement.setInt(index, (Integer)value);
        } else if (value instanceof Long) {
            statement.setLong(index, (Long)value);
        } else if (value instanceof Double) {
            statement.setDouble(index, (Double)value);
        } else if (value instanceof Float) {
            statement.setDouble(index, ((Float)value).doubleValue());
        } else if (value instanceof Boolean) {
            statement.setBoolean(index, (Boolean)value);
        }
        else if (value == null && columnName != null) {
            TableColumnTypesEnum tableColumnType = TableColumnTypesEnum.valueOf(columnName.toUpperCase());
            statement.setNull( index, tableColumnType.getSqlType() );
        }
        else{
            _logger.warn("Value [{}] of class [{}] with index [{}] was not set", value,
                                    value != null ? value.getClass().getName() : "n/a", index);
        }
    }

    private String getDbType(Object value) {
        if (value instanceof String) {
            return dbTypeString;
        }
        if (value instanceof Timestamp) {
            return "TIMESTAMP";
        }
        if (value instanceof Boolean) {
            return "BOOLEAN";
        }
        if (value instanceof Number) {
            if (value instanceof Long) {
                return "BIGINT";
            }
            if (value instanceof Integer) {
                return "INTEGER";
            }
            if (value instanceof Short) {
                return "SMALLINT";
            }
            if (value instanceof Double) {
                return "REAL";
            }
            if (value instanceof Float) {
                return "REAL";
            }

            return "NUMERIC";
        }

        return dbTypeString;
    }

    private void createTable(Connection con, String tableName, Object value, MetricTagsSnapshot tags) {
        try (Statement statement = con.createStatement()) {
            String sqlCreateTable = generateCreateTableQuery(tableName, value, tags);
            statement.executeUpdate(sqlCreateTable);
            _logger.debug("Table [{}] successfully created", tableName);

            String sqlCreateIndex = generateCreateIndexQuery(tableName);
            statement.executeUpdate(sqlCreateIndex);
            _logger.debug("Index for table [{}] successfully created", tableName);
        } catch (SQLException e) {
            _logger.warn("Failed to create table {}", tableName, e);
        }
    }

    private void addMissingColumns(Connection con, String tableName, Map<String, Object> tags) {
        try {
            Map<String, String> missingColumns = calcMissingColumns(con, tableName, tags);
            missingColumns.forEach((columnName, columnType) -> {
                String sql = "ALTER TABLE " + tableName + " ADD " + columnName + " " + columnType;
                _logger.debug("Add column query: [{}]", sql);
                try (Statement statement = con.createStatement()) {
                    statement.executeUpdate(sql);
                    _logger.debug("Added new column [{} {}] to table {}", columnName, columnType, tableName);
                } catch (SQLSyntaxErrorException e) {
                    //since sometimes at teh same times can be fet attempts to add the same column to the same table
                    if (e.getMessage() == null || !e.getMessage().contains("object name already exists in statement")) {
                        _logger.warn("Failed to execute add column query [{}]", sql, e);
                    }
                } catch (SQLException e) {
                    _logger.warn("Failed to execute add column query: [{}]", sql, e);
                }
            });
        } catch (SQLException e) {
            _logger.warn("Failed to add missing columns to table {}", tableName, e);
        }
    }

    private Map<String,String> calcMissingColumns(Connection con, String tableName, Map<String, Object> tags) throws SQLException {
        Set<String> existingColumns = new HashSet<>();
        try (ResultSet rs = con.getMetaData().getColumns(null, null, tableName, null)) {
            while (rs.next()) {
                existingColumns.add(rs.getString("COLUMN_NAME").toUpperCase());
            }
        }

        PredefinedSystemMetrics predefinedSystemMetrics = PredefinedSystemMetrics.valueOf(tableName);
        List<String> columnForInsert = predefinedSystemMetrics.getColumns();

        Map<String,String> missingColumns = new LinkedHashMap<>(); //preserve insertion order
        tags.forEach((name, value) -> {
            if (!existingColumns.contains(name.toUpperCase())) {
                if( columnForInsert == null || columnForInsert.contains( name ) ) {
                    missingColumns.put(name, getDbType(value));
                }
            }
        });

        _logger.debug("Missing columns: {}", missingColumns);
        return missingColumns;
    }

    private String generateInsertQuery(String tableName, long timestamp, Object value, MetricTagsSnapshot tags, List<Object> values, List<String> columnsList ) {
        StringJoiner columns = new StringJoiner(",");
        StringJoiner parameters = new StringJoiner(",");

        addColumnNameAndValue( "TIME", new Timestamp(timestamp), columns, columnsList, parameters, values );

        PredefinedSystemMetrics predefinedSystemMetrics = PredefinedSystemMetrics.valueOf(tableName);
        List<String> columnForInsert = predefinedSystemMetrics.getColumns();

        tags.getTags().forEach((k, v) -> {
            if( columnForInsert == null || columnForInsert.contains( k ) ){
                addColumnNameAndValue( k, v, columns, columnsList, parameters, values );
                if( k == null ){
                    _logger.warn( "Null column name using while inserting row into table {}", tableName );
                }
                if( v == null ){
                    _logger.warn( "Null [{}] value using while inserting row into table {}", k, tableName );
                }
            }
        });

        if( predefinedSystemMetrics == PredefinedSystemMetrics.JVM_MEMORY_HEAP_USED_BYTES ||
                predefinedSystemMetrics == PredefinedSystemMetrics.JVM_MEMORY_HEAP_USED_PERCENT ||
                predefinedSystemMetrics == PredefinedSystemMetrics.PROCESS_CPU_USED_PERCENT ){

            addMissingNullColumnsToVmMetricsIfNeeded( values, columns, parameters, tags.getTags(), columnsList );
        }

        addColumnNameAndValue( "VALUE", value, columns, columnsList, parameters, values );
        if( value == null ) {
            _logger.warn("Null VALUE using while inserting row into table {}", tableName);
        }

        String result = "INSERT INTO " + tableName + " (" + columns + ") VALUES (" + parameters + ")";
        _logger.debug("Generated insert query: {}", result);
        return result;
    }

    private void addMissingNullColumnsToVmMetricsIfNeeded(List<Object> values, StringJoiner columns, StringJoiner parameters, Map<String,Object> tags, List<String> columnsList ) {

        for( String column : VM_TABLE_COLUMNS_WITH_POSSIBLE_NULL_VALUES){
            if( !tags.containsKey( column.toLowerCase() ) ){
                addColumnNameAndValue( column, null, columns, columnsList, parameters, values );
            }
        }
    }

    private void addColumnNameAndValue( String columnName, Object value, StringJoiner columns,
                                       List<String> columnsList, StringJoiner parameters, List<Object> values ) {
        columns.add( columnName );
        columnsList.add( columnName );
        parameters.add("?");
        values.add( value );
    }

    private String generateCreateTableQuery(String tableName, Object value, MetricTagsSnapshot tags) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE CACHED TABLE ").append(tableName).append(" (");
        sb.append("TIME TIMESTAMP,");

        PredefinedSystemMetrics predefinedSystemMetrics = PredefinedSystemMetrics.valueOf(tableName);
        List<String> columnForInsert = predefinedSystemMetrics.getColumns();

        tags.getTags().forEach((columnName, columnValue) ->
            {
                if( columnForInsert == null || columnForInsert.contains( columnName ) ) {
                    sb.append(columnName).append(' ').append(getDbType(columnValue)).append(',');
                }
            }
        );

        sb.append("VALUE ").append(getDbType(value));
        sb.append(')');

        String result = sb.toString();
        _logger.debug("create table query: [{}]", result);
        return result;
    }

    private String generateCreateIndexQuery(String tableName) {
        String sql = "CREATE INDEX gsindex_" + tableName + " ON " + tableName + " ( TIME ASC )";//SPACE_ACTIVE
        _logger.debug("Creating index for table [{}] by executing [{}]", tableName, sql);
        return  sql;
    }
}
