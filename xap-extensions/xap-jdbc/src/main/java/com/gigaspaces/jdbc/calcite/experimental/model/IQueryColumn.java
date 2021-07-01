package com.gigaspaces.jdbc.calcite.experimental.model;

import com.gigaspaces.jdbc.calcite.experimental.ResultSupplier;
import com.gigaspaces.jdbc.model.table.TableContainer;

public interface IQueryColumn extends Comparable<IQueryColumn> {
    String UUID_COLUMN = "UID";

    int getColumnOrdinal();

    String getName();

    String getAlias();

    boolean isUUID();

    ResultSupplier getResultSupplier();

    Object getCurrentValue();

    Class<?> getReturnType();

    IQueryColumn create(String columnName, String columnAlias, int columnOrdinal);
}
