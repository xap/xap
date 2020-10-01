package com.j_spaces.jdbc;


import java.util.List;

@com.gigaspaces.api.InternalApi
public class FunctionCallColumn extends SelectColumn {

    public FunctionCallColumn() {
    }

    public FunctionCallColumn(String columnPath, List params) {
        super(columnPath);
        //@todo barak
    }

}
