package com.j_spaces.jdbc;


import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.query.sql.functions.SqlFunction;
import com.gigaspaces.query.sql.functions.SqlFunctionExecutionContext;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

@com.gigaspaces.api.InternalApi
public class FunctionCallColumn extends SelectColumn {
    private static final long serialVersionUID = 1L;

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

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        IOUtils.writeList(out, params);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        params = IOUtils.readList(in);
    }
}
