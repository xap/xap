package com.j_spaces.jdbc;


import com.gigaspaces.internal.transport.IEntryPacket;

import java.io.Serializable;
import java.util.List;
import java.util.function.Function;

@com.gigaspaces.api.InternalApi
public class FunctionCallColumn<R extends Serializable, T extends Serializable> extends SelectColumn {

    private List params;

    private Function<R, T> f;

    public FunctionCallColumn() {
    }

    public FunctionCallColumn(String functionName, List params) {
        super(params.get(0).toString());
        this.setFunctionName(functionName);
        this.params = params;
        this.params.remove(0); // removes first param which is always the column name

        if (functionName.equalsIgnoreCase("REPLACE")) {
            f = (obj) -> (T) obj.toString().replace(params.get(0).toString(), params.get(1).toString());
        } else {
            throw new RuntimeException("Unknown function [" + functionName + "]");
        }
    }

    @Override
    public Object getFieldValue(IEntryPacket entry) {
        R fValue = (R) super.getFieldValue(entry);
        return f.apply(fValue);
    }

    public Function<R, T> getF() {
        return f;
    }
}
