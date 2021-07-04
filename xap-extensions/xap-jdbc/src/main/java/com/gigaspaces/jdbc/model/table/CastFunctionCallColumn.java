package com.gigaspaces.jdbc.model.table;

import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.query.sql.functions.SqlFunction;
import com.gigaspaces.query.sql.functions.SqlFunctionExecutionContextWithType;
import com.j_spaces.jdbc.SQLFunctions;

import java.util.List;

public class CastFunctionCallColumn extends FunctionCallColumn{
    private String type;

    public CastFunctionCallColumn(List<IQueryColumn> params, String columnName, String functionName, String columnAlias, boolean isVisible, int columnOrdinal, String type) {
        super(params, columnName, functionName, columnAlias, isVisible, columnOrdinal);
        this.type = type;
    }

    public String getType() {
        return type;
    }

    @Override
    public Object getValue(IEntryPacket entryPacket) {
        SqlFunction sqlFunction = SQLFunctions.getBuiltInFunction(getFunctionName());
        if(sqlFunction != null){
            return sqlFunction.apply(new SqlFunctionExecutionContextWithType() {
                @Override
                public int getNumberOfArguments() {
                    return params.size();
                }

                @Override
                public Object getArgument(int index) {
                    return params.get(index).getValue(entryPacket);
                }

                @Override
                public String getType(){
                    return type;
                }
            });
        }
        throw new RuntimeException("Unknown function [" + getFunctionName() + "]");
    }
}
