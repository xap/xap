package com.gigaspaces.sql.aggregatornode.netty.query;

import org.apache.calcite.sql.SqlNode;

public interface Statement extends AutoCloseable {
    String getName();

    String getQueryString();

    SqlNode getQuery();

    StatementDescription getDescription();
}
