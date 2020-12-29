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

import com.gigaspaces.api.InternalApi;

import java.sql.JDBCType;

/**
 * @since 15.5
 */
@InternalApi
public enum TableColumnTypesEnum implements TableColumnNames {

    TIME( TIME_COLUMN_NAME, JDBCType.TIMESTAMP ),
    PU_INSTANCE_ID( PU_INSTANCE_ID_COLUMN_NAME,JDBCType.VARCHAR ),
    SPACE_INSTANCE_ID(SPACE_INSTANCE_ID_COLUMN_NAME, JDBCType.VARCHAR ),
    DATA_TYPE_NAME (DATA_TYPE_NAME_COLUMN_NAME, JDBCType.VARCHAR ),
    SPACE_NAME( SPACE_NAME_COLUMN_NAME, JDBCType.VARCHAR ),
    PU_NAME( PU_NAME_COLUMN_NAME, JDBCType.VARCHAR ),
    INDEX( INDEX_COLUMN_NAME, JDBCType.VARCHAR ),
    HOST( HOST_COLUMN_NAME, JDBCType.VARCHAR ),
    PID( PID_COLUMN_NAME, JDBCType.VARCHAR ),
    PROCESS_NAME( PROCESS_NAME_COLUMN_NAME, JDBCType.VARCHAR ),
    IP( IP_COLUMN_NAME, JDBCType.VARCHAR );

    private final String columnName;
    private final JDBCType jdbcType;

    TableColumnTypesEnum(String columnName, JDBCType jdbcType ) {
        this.columnName = columnName;
        this.jdbcType = jdbcType;
    }

    public String getColumnName() {
        return columnName;
    }

    public int getSqlType() {
        return jdbcType.getVendorTypeNumber();
    }

    public JDBCType getJdbcType() {
        return jdbcType;
    }

    public String getSqlTypeName() {
        return jdbcType.getName();
    }

    public static int getSqlType(String columnName) {
        return TableColumnTypesEnum.valueOf(columnName.toUpperCase()).getSqlType();
    }

    public static JDBCType getJDBCType(String columnName) {
        return TableColumnTypesEnum.valueOf(columnName.toUpperCase()).getJdbcType();
    }
}