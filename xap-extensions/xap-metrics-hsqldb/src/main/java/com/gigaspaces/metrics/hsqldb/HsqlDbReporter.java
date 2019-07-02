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
import com.gigaspaces.start.SystemInfo;
import com.gigaspaces.start.manager.XapManagerClusterInfo;
import com.gigaspaces.start.manager.XapManagerConfig;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * @author Evgeny
 * @since 15.0
 */
public class HsqlDbReporter extends MetricReporter {

    private static final Logger _logger = Logger.getLogger( HsqlDbReporter.class.getName() );
    private Connection con = null;


    public HsqlDbReporter(HsqlDBReporterFactory factory) {
        super(factory);

        String dbName = factory.getDbName();
        String driverClassName = factory.getDriverClassName();
        String password = factory.getPassword();
        int port = factory.getPort();
        String username = factory.getUsername();


        XapManagerClusterInfo managerClusterInfo = SystemInfo.singleton().getManagerClusterInfo();
        if( !managerClusterInfo.isEmpty() ) {
            XapManagerConfig[] servers = managerClusterInfo.getServers();

            if( servers.length > 0 ) {
                String firstManagerHost = servers[0].getHost();
                // EXAMPLE of url: "jdbc:hsqldb:hsql://localhost:9101/metricsdb"
                final String url = "jdbc:hsqldb:hsql://" + firstManagerHost + ":" + port + "/" + dbName;
                try {
                    (new Thread(
                        () -> con = createConnection(driverClassName, url, username, password)))
                        .start();
                } catch (Exception e) {
                    if (_logger.isLoggable(Level.SEVERE)) {
                        _logger.log(Level.SEVERE, e.toString(), e);
                    }
                }
            }
        }
    }

    private Connection createConnection( String driverClassName, String url, String username, String password ) {
        Connection con = null;
        try {
            Class.forName( driverClassName );
            while( con == null ) {
                try {
                    con = DriverManager.getConnection(url, username, password);
                    _logger.info( "Connection to [" + url + "] successfully created" );
                }
                catch( Exception e ){
                    if( _logger.isLoggable( Level.WARNING ) ){
                        _logger.log( Level.WARNING, e.toString() );
                    }
                }
                if( con == null ){
                    try {
                        Thread.sleep( 1 * 1000 );
                    } catch (InterruptedException e) {

                    }
                }
            }

            DatabaseMetaData dbm = con.getMetaData();
            String catalog = con.getCatalog();
            _logger.info( "Before create tables" );
            createRequiredTablesIfMissing(con, catalog, dbm);
            _logger.info( "After create tables" );
        }
        catch (Exception e) {
            if( _logger.isLoggable( Level.SEVERE ) ){
                _logger.log( Level.SEVERE, e.toString(), e );
            }
        }
        finally {
/*            try {
                if (con != null) {
                    con.close();
                }
            } catch (Exception ignore) {
            }*/
        }

        return con;
    }


    public void report(List<MetricRegistrySnapshot> snapshots) {
        super.report(snapshots);
        //flush();
    }

    @Override
    protected void report(MetricRegistrySnapshot snapshot, MetricTagsSnapshot tags, String key, Object value) {
        // Save length before append:
       //String row1 = "insert into " + realDbTableName + " values('23','AABBAABB','Address','NY','AB',23500)";

        _logger.info( "Report, con=" + con + ", key=" + key );
        if( con == null ){
            return;
        }


        final String dbTableName = key.toUpperCase();
        final String realDbTableName = removeInvalidTableNameCharacters( dbTableName );

        Set<Map.Entry<String, Object>> entries = tags.getTags().entrySet();
        StringBuilder columns = new StringBuilder();
        StringBuilder values = new StringBuilder();
        Map<Integer,Object> insertRowQueryValues = new HashMap<>();

        columns.append( "TIME" );
        columns.append( ',' );

        values.append( '?' );
        values.append( ',' );

        insertRowQueryValues.put( 1, new Timestamp( snapshot.getTimestamp() ) );

        int index = 2;
        for( Map.Entry<String, Object> entry : entries ){
            String entryKey = entry.getKey();
            Object entryValue = entry.getValue();

            _logger.info( ">>> KEY=" + entryKey + ", value class name:" + entryValue.getClass().getName() + ", value=" + entryValue);

            columns.append( entryKey );
            columns.append( ',' );

            values.append( '?' );//transformValue( entryValue ) );
            values.append( ',' );

            insertRowQueryValues.put( index, transformValue( entryValue ) );

            index++;
        }

        columns.append( "VALUE" );
        values.append( '?' );
        insertRowQueryValues.put( index, value );

        final String insertSQL = "INSERT INTO " + realDbTableName + " (" + columns + ") VALUES (" + values + ")";

        _logger.info( "insert row query before setting values [" + insertSQL + "]" );

        try {
            PreparedStatement preparedStatement = con.prepareStatement( insertSQL );

            Set<Map.Entry<Integer, Object>> insertRowQueryEntries = insertRowQueryValues.entrySet();
            for( Map.Entry<Integer, Object> entry : insertRowQueryEntries ){
                Integer paramIndex = entry.getKey();
                Object paramValue = entry.getValue();
                if( paramValue instanceof String ) {
                    preparedStatement.setString( paramIndex, paramValue.toString() );
                }
                else if( paramValue instanceof Timestamp ){
                    preparedStatement.setTimestamp( paramIndex, ( Timestamp )paramValue );
                }
                else if( paramValue instanceof Integer ){
                    preparedStatement.setInt( paramIndex, ( Integer )paramValue );
                }
                else if( paramValue instanceof Long ){
                    preparedStatement.setLong( paramIndex, ( Long )paramValue );
                }
                else if( paramValue instanceof Double ){
                    preparedStatement.setDouble( paramIndex, ( Double )paramValue );
                }
                else if( paramValue instanceof Float ){
                    preparedStatement.setFloat( paramIndex, ( Float )paramValue );
                }
            }
            preparedStatement.executeUpdate();
        }
        catch (SQLException e) {
            if( _logger.isLoggable( Level.SEVERE ) ){
                _logger.log( Level.SEVERE, e.toString(), e );
            }
        }


        /* WORKING STATEMENT

        Set<Map.Entry<String, Object>> entries = tags.getTags().entrySet();
        StringBuilder columns = new StringBuilder();
        StringBuilder values = new StringBuilder();


        columns.append( "TIME" );
        columns.append( ',' );

        values.append(new Timestamp( snapshot.getTimestamp() ) );
        values.append( ',' );

        for( Map.Entry<String, Object> entry : entries ){
            String entryKey = entry.getKey();
            Object entryValue = entry.getValue();

            _logger.info( ">>> KEY=" + entryKey + ", value class name:" + entryValue.getClass().getName() + ", value=" + entryValue);

            columns.append( entryKey );
            columns.append( ',' );

            values.append( transformValue( entryValue ) );
            values.append( ',' );
        }

        columns.append( "VALUE" );
        values.append( transformValue( value ) );

        try {
            String insertSQL = "INSERT INTO " + realDbTableName + " (" + columns + ") VALUES (" + values + ")";
            _logger.info( "insert row query before insert [" + insertSQL + "]" );
            Statement statement = con.createStatement();
            statement.executeUpdate( insertSQL );
        }
        catch (SQLException e) {
            if( _logger.isLoggable( Level.SEVERE ) ){
                _logger.log( Level.SEVERE, e.toString(), e );
            }
        }
*/

        //String insertSQL = "INSERT INTO " + realDbTableName + " (StrCol1, StrCol2) VALUES (?, ?)";

/*  Prepared statement
        String insertSQL = "INSERT INTO " + realDbTableName + " (" + columns + ") VALUES (" + values + ")";
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = con.prepareStatement( insertSQL );


            preparedStatement.setString(1, "Val 1");
            preparedStatement.setString(2, "Val 2");
            preparedStatement.executeUpdate();
        }
        catch (SQLException e) {
            if( _logger.isLoggable( Level.SEVERE ) ){
                _logger.log( Level.SEVERE, e.toString(), e );
            }
        }*/
    }

    private Object transformValue( Object value ){
        if( value instanceof String ){
            return "'" + value + "'";
        }

        return value;
    }

    private String removeInvalidTableNameCharacters(String dbTableName) {
        return dbTableName.replace( '-', '_' );
    }

    @Override
    public void close() {
        super.close();
    }


    private void createRequiredTablesIfMissing(Connection con, String catalog, DatabaseMetaData dbm) {

        /*Test: create table, insert row and select table rows*/

        String tableName = "account";
        createTable(tableName,
                    "(ID VARCHAR(5), "
                    + "PAN_NUMBER  VARCHAR(10), "
                    + "ADDRESS VARCHAR(40), "
                    + "CITY VARCHAR(35), "
                    + "STATE VARCHAR(2), "
                    + "PINCODE integer)",
                    con, catalog, dbm);

        tableName = "jvm_memory_gc_count";
        createTable(tableName,
                    "(time timestamp, "
                    + "host  VARCHAR(40), "
                    + "ip VARCHAR(15), "
                    + "pid VARCHAR(15), "
                    + "process_name VARCHAR(15), "
                    + "pu_instance_id VARCHAR(30), "
                    + "pu_name VARCHAR(30), "
                    + "value integer,)",
                    con, catalog, dbm);

        tableName = "jvm_memory_gc_time";
        createTable(tableName,
                    "(time timestamp, "
                    + "host VARCHAR(40), "
                    + "ip VARCHAR(15), "
                    + "pid VARCHAR(15), "
                    + "process_name VARCHAR(15), "
                    + "pu_instance_id VARCHAR(30), "
                    + "pu_name VARCHAR(30), "
                    + "value integer,)",
                    con, catalog, dbm);

        tableName = "jvm_memory_heap_committed_bytes";//"jvm_memory_heap_committed-bytes";
        createTable(tableName,
                    "(time timestamp, "
                    + "host VARCHAR(40), "
                    + "ip VARCHAR(15), "
                    + "pid VARCHAR(15), "
                    + "process_name VARCHAR(15), "
                    + "pu_instance_id VARCHAR(30), "
                    + "pu_name VARCHAR(30), "
                    + "value integer,)",
                    con, catalog, dbm);

        tableName = "jvm_memory_heap_used_bytes";//"jvm_memory_heap_used-bytes";
        createTable(tableName,
                    "(time timestamp, "
                    + "host VARCHAR(40), "
                    + "ip VARCHAR(15), "
                    + "pid VARCHAR(15), "
                    + "process_name VARCHAR(15), "
                    + "pu_instance_id VARCHAR(30), "
                    + "pu_name VARCHAR(30), "
                    + "value integer,)",
                    con, catalog, dbm);

        tableName = "jvm_memory_non_heap_committed_bytes";//"jvm_memory_non-heap_committed-bytes";
        createTable(tableName,
                    "(time timestamp, "
                    + "host VARCHAR(40), "
                    + "ip VARCHAR(15), "
                    + "pid VARCHAR(15), "
                    + "process_name VARCHAR(15), "
                    + "pu_instance_id VARCHAR(30), "
                    + "pu_name VARCHAR(30), "
                    + "value integer,)",
                    con, catalog, dbm);

        tableName = "jvm_memory_non_heap_used_bytes";//"jvm_memory_non-heap_used-bytes"
        createTable(tableName,
                    "(time timestamp, "
                    + "host VARCHAR(40), "
                    + "ip VARCHAR(15), "
                    + "pid VARCHAR(15), "
                    + "process_name VARCHAR(15), "
                    + "pu_instance_id VARCHAR(30), "
                    + "pu_name VARCHAR(30), "
                    + "value integer,)",
                    con, catalog, dbm);
    }

    private void createTable( String tableName, String columnsInfo, Connection con, String catalog, DatabaseMetaData dbm ) {

        final String realDbTableName = tableName.toUpperCase();

        try {
            ResultSet tables = dbm.getTables(catalog, null, realDbTableName, null);
            if (tables.next()) {
                // Table exists
                _logger.info("Table [" + realDbTableName + "] exists");
            } else {

                String sql = "CREATE TABLE " + realDbTableName + " " + columnsInfo;
/*
                                 " (ID VARCHAR(5), "
                                 + "PAN_NUMBER  VARCHAR(10), "
                                 + "ADDRESS VARCHAR(40), "
                                 + "CITY VARCHAR(35), "
                                 + "STATE VARCHAR(2), "
                                 + "PINCODE integer)";
*/
                int i = con.createStatement().executeUpdate(sql);
                _logger.info("Table [" + realDbTableName + "] created");
            }
        } catch (SQLException e) {
            _logger.log(Level.SEVERE,
                        "Failed to create table [" + realDbTableName + "] : " + e.getMessage(), e);
        }
    }
}