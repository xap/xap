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

import java.io.Closeable;
import java.io.IOException;
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

    public DatabaseMetaDataReader(String url) throws SQLException {
        this(DriverManager.getConnection(url));
    }

    public DatabaseMetaDataReader(String url, Properties properties) throws SQLException {
        this(DriverManager.getConnection(url, properties));
    }

    public DatabaseMetaDataReader(String url, String user, String password) throws SQLException {
        this(DriverManager.getConnection(url, user, password));
    }

    private DatabaseMetaDataReader(Connection connection) throws SQLException {
        this.connection = connection;
        this.metaData = connection.getMetaData();
    }

    @Override
    public void close() throws IOException {
        try {
            this.connection.close();
        } catch (SQLException e) {
            throw new IOException("Failed to close connection", e);
        }
    }

    public Connection getConnection() {
        return connection;
    }

    public List<TableInfo> getTables() throws SQLException {
        return getTables(new TableQuery());
    }

    public List<TableInfo> getTables(TableQuery query) throws SQLException {
        List<TableInfo> result = new ArrayList<>();
        for (TableId id : getTablesIds(query)) {
            result.add(getTableInfo(id));
        }
        return result;
    }

    public List<TableId> getTablesIds() throws SQLException {
        return getTablesIds(new TableQuery());
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

    public TableInfo getTableInfo(TableId table) throws SQLException {
        return new TableInfo(table, getTableColumns(table), getTablePrimaryKey(table), getTableIndexes(table));
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
                    result.put(indexName, new IndexInfo(indexName, nonUnique));
                }
                result.get(indexName).getColumns().add(rs.getString("COLUMN_NAME"));
            }
        }
        return new ArrayList<>(result.values());
    }
}
