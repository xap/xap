package com.gigaspaces.jdbc.model.table;

import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.query.sql.functions.SqlFunction;
import com.gigaspaces.query.sql.functions.SqlFunctionExecutionContext;
import com.j_spaces.jdbc.SQLFunctions;

import java.lang.reflect.Array;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class FunctionCallColumn implements IQueryColumn{
    private final List<IQueryColumn> params;
    private final String columnName;
    private final String functionName;
    private final String columnAlias;
    private final boolean isVisible;
    private final int columnOrdinal;

    public FunctionCallColumn(List<IQueryColumn> params, String columnName, String functionName, String columnAlias, boolean isVisible, int columnOrdinal) {
        this.params = params;
        this.columnName = columnName;
        this.functionName = functionName;
        this.columnAlias = columnAlias;
        this.isVisible = isVisible;
        this.columnOrdinal = columnOrdinal;
    }

    @Override
    public int getColumnOrdinal() {
        return columnOrdinal;
    }

    @Override
    public String getName() {
        return functionName + "(" + String.join(", ", params.stream().map(IQueryColumn::getName).collect(Collectors.toList())) + ")";
    }

    @Override
    public String getAlias() {
        return columnAlias;
    }

    @Override
    public boolean isVisible() {
        return isVisible;
    }

    @Override
    public boolean isUUID() {
        return false;
    }

    @Override
    public TableContainer getTableContainer() {
        throw new UnsupportedOperationException("Unsupported method getName");
    }

    @Override
    public Object getCurrentValue() {
        throw new UnsupportedOperationException("Unsupported method getName");
    }

    @Override
    public Class<?> getReturnType() {
        return Object.class;
    }

    @Override
    public IQueryColumn create(String columnName, String columnAlias, boolean isVisible, int columnOrdinal) {
        return null;
    }

    @Override
    public int compareTo(IQueryColumn o) {
        return 0;
    }

    public String getFunctionName() {
        return functionName;
    }

    @Override
    public Object getValue(IEntryPacket entryPacket) {
        SqlFunction sqlFunction = SQLFunctions.getBuiltInFunction(getFunctionName());
        if(sqlFunction != null){
            return sqlFunction.apply(new SqlFunctionExecutionContext() {
                @Override
                public int getNumberOfArguments() {
                    return params.size();
                }

                @Override
                public Object getArgument(int index) {
                    return params.get(index).getValue(entryPacket);
                }
            });
        }
        throw new RuntimeException("Unknown function [" + getFunctionName() + "]");
    }
}
