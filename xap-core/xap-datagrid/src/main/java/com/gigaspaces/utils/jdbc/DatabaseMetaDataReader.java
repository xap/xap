/*
 * Copyright (c) 2008-2019, GigaSpaces Technologies, Inc. All Rights Reserved.
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

package com.gigaspaces.utils.jdbc;

import com.gigaspaces.classloader.CustomURLClassLoader;
import com.gigaspaces.start.ClasspathBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.sql.*;
import java.util.*;

/**
 * API for reading metadata from a database using JDBC.
 *
 * @author Niv Ingberg
 * @since 15.2.0
 */
public class DatabaseMetaDataReader implements Closeable {
    private final Connection connection;
    private final DatabaseMetaData metaData;
    private final URLClassLoader driverClassLoader;
    private final Logger logger = LoggerFactory.getLogger(DatabaseMetaDataReader.class);

    public DatabaseMetaDataReader(String url, Properties properties) throws SQLException {
        this(builder(url).properties(properties));
    }

    private DatabaseMetaDataReader(Builder builder) throws SQLException {
        this.connection = builder.driver != null
                ? builder.driver.connect(builder.url, builder.properties)
                : DriverManager.getConnection(builder.url, builder.properties);
        if (this.connection == null) {
            throw new SQLException("Connection to the database failed");
        }
        this.metaData =  connection.getMetaData() ;
        this.driverClassLoader = builder.driverClassLoader;
    }

    public static Builder builder(String url) {
        return new Builder(url);
    }

    @Override
    public void close() throws IOException {
        try {
            this.connection.close();
        } catch (SQLException e) {
            throw new IOException("Failed to close connection", e);
        }
        if (driverClassLoader != null) {
            driverClassLoader.close();
        }
    }

    public Connection getConnection() {
        return connection;
    }

    public List<TableInfo> getTables() throws SQLException {
        return getTables(getDefaultTableQuery());
    }

    private TableQuery getDefaultTableQuery() throws SQLException {
        return new TableQuery().setCatalog(connection.getCatalog()).setSchemaPattern(connection.getSchema());
    }

    public List<TableInfo> getTables(TableQuery query) throws SQLException {
        List<TableInfo> result = new ArrayList<>();
        for (TableId id : getTablesIds(query)) {
            TableInfo tableInfo = getTableInfo(id);
            if(tableInfo != null) {
                result.add(tableInfo);
            }
        }
        return result;
    }

    public List<TableId> getTablesIds() throws SQLException {
        return getTablesIds(getDefaultTableQuery());
    }

    public List<TableId> getTablesIds(TableQuery query) throws SQLException {
        List<TableId> result = new ArrayList<>();
        try (ResultSet rs = metaData.getTables(query.getCatalog(), query.getSchemaPattern(), query.getTableNamePattern(), query.getTypes())) {
            while (rs.next()) {
                result.add(new TableId(rs));
            }
        }
        return result;
    }

    public List<String> getTableTypes() throws SQLException {
        List<String> result = new ArrayList<>();
        try (ResultSet rs = metaData.getTableTypes()) {
            while (rs.next()) {
                result.add(rs.getString("TABLE_TYPE"));
            }
        }
        return result;
    }

    public TableInfo getTableInfo(TableId table) {
        try {
            return new TableInfo(table, getTableColumns(table), getTablePrimaryKey(table), getTableIndexes(table));
        } catch (SQLException e) {
            logger.warn("Table "+table+" SQL state: "+e.getSQLState()+" "+e.getErrorCode(),e);
        } catch (Exception t) {
            logger.warn("Table "+table+" "+t.getMessage(),t);
        }
        return null;
    }

    public List<ColumnInfo> getTableColumns(TableId table) throws SQLException {
        List<ColumnInfo> result = new ArrayList<>();
        try (ResultSet rs = metaData.getColumns(table.getCatalog(), table.getSchema(), table.getName(), "%")) {
            while (rs.next()) {
                result.add(new ColumnInfo(rs));
            }
        }
        return result;
    }

    public List<String> getTablePrimaryKey(TableId table) throws SQLException {
        List<String> result = new ArrayList<>();
        try (ResultSet rs = metaData.getPrimaryKeys(table.getCatalog(), table.getSchema(), table.getName())) {
            while (rs.next()) {
                result.add(rs.getString("COLUMN_NAME"));
            }
        }
        return result;
    }

    public List<IndexInfo> getTableIndexes(TableId table) throws SQLException {
        Map<String, IndexInfo> result = new LinkedHashMap<>();
        try (ResultSet rs = metaData.getIndexInfo(table.getCatalog(), table.getSchema(), table.getName(), false, false)) {
            while (rs.next()) {
                String indexName = rs.getString("INDEX_NAME");
                if (!result.containsKey(indexName)) {
                    boolean nonUnique = rs.getBoolean("NON_UNIQUE");
                    result.put(indexName, new IndexInfo(indexName, "EQUAL", nonUnique));
                }
                result.get(indexName).getColumns().add(rs.getString("COLUMN_NAME"));

            }
        }
        return new ArrayList<>(result.values());
    }

    public static class Builder {

        private final String url;
        private final Properties properties = new Properties();
        private Driver driver;
        private URLClassLoader driverClassLoader;

        public Builder(String url) {
            this.url = url;
        }

        public DatabaseMetaDataReader build() throws SQLException {
            return new DatabaseMetaDataReader(this);
        }

        public Builder properties(Properties properties) {
            this.properties.putAll(properties);
            return this;
        }

        public Builder property(String key, String value) {
            this.properties.setProperty(key, value);
            return this;
        }

        public Builder credentials(String user, String password) {
            if (user != null) {
                properties.setProperty("user", user);
            }
            if (password != null) {
                properties.setProperty("password", password);
            }
            return this;
        }

        public Builder driver(Driver driver) {
            this.driver = driver;
            return this;
        }

        public Builder driver(String driverClass) throws ReflectiveOperationException, IOException  {
            return driver(driverClass, Collections.EMPTY_LIST);
        }

        public Builder driver(String driverClass, Path driverClasspath) throws ReflectiveOperationException, IOException  {
            return driver(driverClass, Collections.singleton(driverClasspath));
        }

        public Builder driver(String driverClass, Collection<Path> driverClasspath) throws ReflectiveOperationException, IOException {
            ClasspathBuilder cpBuilder = new ClasspathBuilder();
            for (Path path : driverClasspath) {
                cpBuilder.appendJars(path);
            }

            try {
                driver( driverClass, cpBuilder.toURLsArray() );
            } catch (MalformedURLException e) {
                throw new IOException("Failed to create classpath from " + driverClasspath);
            }

            return this;
        }

        public Builder driver(String driverClass, URL[] classpath) throws ReflectiveOperationException {

            this.driverClassLoader = new CustomURLClassLoader("cl-jdbc-" + driverClass, classpath, Thread.currentThread().getContextClassLoader());
            this.driver = driverClassLoader.loadClass(driverClass).asSubclass(Driver.class).newInstance();
            return this;
        }
    }

}
