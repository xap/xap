package com.gigaspaces.jdbc.calcite.experimental.model;

import com.gigaspaces.internal.transport.IEntryPacket;

import com.gigaspaces.jdbc.calcite.experimental.ResultSupplier;
import com.gigaspaces.query.sql.functions.SqlFunction;
import com.gigaspaces.query.sql.functions.SqlFunctionExecutionContext;
import com.j_spaces.jdbc.SQLFunctions;

import java.util.List;
import java.util.stream.Collectors;

public class FunctionColumn implements IQueryColumn {
    protected final List<IQueryColumn> params;
    protected final String columnName;
    protected final String functionName;
    protected final String columnAlias;
    protected final boolean isVisible;

    public FunctionColumn(List<IQueryColumn> params, String columnName, String functionName, String columnAlias, boolean isVisible) {
        this.params = params;
        this.columnName = columnName;
        this.functionName = functionName;
        this.columnAlias = columnAlias;
        this.isVisible = isVisible;
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
    public boolean isUUID() {
        return false;
    }

    @Override
    public ResultSupplier getResultSupplier() {
        throw new UnsupportedOperationException("Unsupported method getName");
    }

    @Override
    public Object getCurrentValue() {
        return getValue(null);
    }

    @Override
    public Class<?> getReturnType() {
        return Object.class;
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
