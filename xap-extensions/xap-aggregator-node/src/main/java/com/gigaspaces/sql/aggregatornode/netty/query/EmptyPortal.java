package com.gigaspaces.sql.aggregatornode.netty.query;

import com.gigaspaces.sql.aggregatornode.netty.utils.Constants;

import java.util.NoSuchElementException;

class EmptyPortal<T> implements Portal<T> {
    private final QueryProviderImpl queryProvider;
    private final String name;
    private final Statement statement;

    EmptyPortal(QueryProviderImpl queryProvider, String name, Statement statement) {
        this.queryProvider = queryProvider;
        this.name = name;
        this.statement = statement;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public Statement getStatement() {
        return statement;
    }

    @Override
    public void close() {
        queryProvider.closeP(name);
    }

    @Override
    public boolean empty() {
        return true;
    }

    @Override
    public RowDescription getDescription() {
        return RowDescription.EMPTY;
    }

    @Override
    public String tag() {
        return Constants.EMPTY_STRING;
    }

    @Override
    public void execute() {
    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public T next() {
        throw new NoSuchElementException();
    }
}
