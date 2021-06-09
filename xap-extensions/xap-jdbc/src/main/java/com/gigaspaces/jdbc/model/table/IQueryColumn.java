package com.gigaspaces.jdbc.model.table;

public interface IQueryColumn extends Comparable<IQueryColumn> {
    String UUID_COLUMN = "UID";

    int getColumnOrdinal();

    String getName();

    String getAlias();

    boolean isVisible();

    boolean isUUID();

    TableContainer getTableContainer();

    Object getCurrentValue();

    Class<?> getReturnType();

    String getNameOrAlias();
}
