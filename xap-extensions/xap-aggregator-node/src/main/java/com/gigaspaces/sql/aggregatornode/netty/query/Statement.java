package com.gigaspaces.sql.aggregatornode.netty.query;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlValidator;

public interface Statement extends AutoCloseable {
    /**
     * @return Statement name
     */
    String getName();

    /**
     * @return Parsed query
     */
    SqlNode getQuery();

    SqlValidator getValidator();

    /**
     * @return Parameters and result row descriptions
     */
    StatementDescription getDescription();
}
