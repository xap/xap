package com.j_spaces.jdbc;


import com.gigaspaces.internal.transport.IEntryPacket;

import java.util.List;
import java.util.function.Function;

@com.gigaspaces.api.InternalApi
public class FunctionCallColumn extends SelectColumn {

    private List params;

    private Function<Object, Object> f;

    public FunctionCallColumn() {
    }

    public FunctionCallColumn(String functionName, List params) {
        super(params.get(0).toString());
        this.setFunctionName(functionName);
        this.params = params;
        this.params.remove(0); // removes first param which is always the column name

        if (functionName.equals("REPLACE")) {
            f = (obj) -> obj.toString().replace(params.get(0).toString(), params.get(1).toString());
        } else {
            throw new RuntimeException("Unknown function [" + functionName + "]");
        }
    }

    @Override
    public Object getFieldValue(IEntryPacket entry) {
        Object fValue = super.getFieldValue(entry);
        return f.apply(fValue);
    }


}
