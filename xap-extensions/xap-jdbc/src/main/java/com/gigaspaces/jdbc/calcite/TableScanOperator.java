package com.gigaspaces.jdbc.calcite;

import com.gigaspaces.internal.client.QueryResultTypeInternal;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;
import com.j_spaces.jdbc.query.QueryTableData;

public class TableScanOperator implements ISQLOperator {
    private final QueryTemplatePacketsHolder queryTemplatePacketsHolder;
    private final ITypeDesc typeDesc;
    private final String tableName;
    private final String tableAlias;


    public TableScanOperator(QueryTemplatePacketsHolder queryTemplatePacketsHolder, ITypeDesc typeDesc) {
        this.queryTemplatePacketsHolder = queryTemplatePacketsHolder;
        this.typeDesc = typeDesc;
        this.tableName = typeDesc.getTypeName();
        this.tableAlias = null;
    }

    @Override
    public QueryTemplatePacketsHolder build() {
        QueryTableData queryTableData = new QueryTableData(this.tableName, this.tableAlias, 0);
        queryTableData.setTypeDesc(typeDesc);
        QueryTemplatePacket queryTemplatePacket = new QueryTemplatePacket(queryTableData, QueryResultTypeInternal.NOT_SET);
        queryTemplatePacketsHolder.addQueryTemplatePacket(queryTemplatePacket);
        return queryTemplatePacketsHolder;
    }

}
