package com.gigaspaces.sql.aggregatornode.netty.query;

public interface Statement extends AutoCloseable {
    String getName();

    String getQuery();

    StatementDescription getDescription();
}
