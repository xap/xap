package com.gigaspaces.jdbc.calcite;

import com.gigaspaces.jdbc.model.table.OrderColumn;

import java.util.List;

public class JoinOperator implements ISQLOperator {
    private final QueryTemplatePacketsHolder queryTemplatePacketsHolder;


    public JoinOperator(QueryTemplatePacketsHolder queryTemplatePacketsHolder, List<OrderColumn> orderColumns) {
        this.queryTemplatePacketsHolder = queryTemplatePacketsHolder;
    }

    @Override
    public QueryTemplatePacketsHolder build() {
        return queryTemplatePacketsHolder;
    }
}
