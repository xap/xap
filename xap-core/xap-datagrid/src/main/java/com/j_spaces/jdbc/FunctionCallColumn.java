package com.j_spaces.jdbc;


import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.query.sql.functions.SqlFunction;
import com.gigaspaces.query.sql.functions.SqlFunctionExecutionContext;
import java.util.List;

@com.gigaspaces.api.InternalApi
public class FunctionCallColumn extends SelectColumn {

    private List params;

    public FunctionCallColumn() {
    }

    public FunctionCallColumn(String functionName, List params) {
        super(params.get(0).toString());
        this.setFunctionName(functionName);
        this.params = params;
    }

    @Override
    public Object getFieldValue(IEntryPacket entry) {
        return apply(super.getFieldValue(entry));
    }

    public Object apply(Object value){
        params.set(0, value);
        SqlFunction sqlFunction = SQLFunctions.getBuiltInFunction(getFunctionName());
        if(sqlFunction != null){
            return sqlFunction.apply(new SqlFunctionExecutionContext() {
                @Override
                public int getNumberOfArguments() {
                    return params.size();
                }

                @Override
                public Object getArgument(int index) {
                    return params.get(index);
                }
            });
        }
        throw new RuntimeException("Unknown function [" + getFunctionName() + "]");
    }
}
