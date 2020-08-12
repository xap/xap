package com.gigaspaces.metrics.hsqldb;

import com.gigaspaces.api.InternalApi;

import java.sql.Types;

/**
 * @since 15.5
 */
@InternalApi
public enum TableColumnTypesEnum implements TableColumnNames {

    TIME( TIME_COLUMN_NAME, Types.TIMESTAMP ),
    PU_INSTANCE_ID( PU_INSTANCE_ID_COLUMN_NAME,Types.VARCHAR ),
    DATA_TYPE_NAME (DATA_TYPE_NAME_COLUMN_NAME, Types.VARCHAR ),
    SPACE_NAME( SPACE_NAME_COLUMN_NAME, Types.VARCHAR ),
    PU_NAME( PU_NAME_COLUMN_NAME, Types.VARCHAR ),
    SPACE_ACTIVE( SPACE_ACTIVE_COLUMN_NAME, Types.BOOLEAN ),
    INDEX( INDEX_COLUMN_NAME, Types.VARCHAR ),
    HOST( HOST_COLUMN_NAME, Types.VARCHAR ),
    PID( PID_COLUMN_NAME, Types.VARCHAR ),
    PROCESS_NAME( PROCESS_NAME_COLUMN_NAME, Types.VARCHAR ),
    IP( IP_COLUMN_NAME, Types.VARCHAR );

    private final String columnName;
    private final int sqlType;

    TableColumnTypesEnum(String columnName, int sqlType ) {
        this.columnName = columnName;
        this.sqlType = sqlType;
    }

    public String getColumnName() {
        return columnName;
    }

    public int getSqlType() {
        return sqlType;
    }
}