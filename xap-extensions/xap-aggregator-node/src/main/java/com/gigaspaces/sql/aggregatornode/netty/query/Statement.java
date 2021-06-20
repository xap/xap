package com.gigaspaces.sql.aggregatornode.netty.query;

import org.apache.calcite.sql.SqlNode;

public interface Statement extends AutoCloseable {
    /**
     * @return Statement name
     */
    String getName();

    /**
     * @return Parsed query
     */
    SqlNode getQuery();

    /**
     * @return Parameters and result row descriptions
     */
    StatementDescription getDescription();
}
