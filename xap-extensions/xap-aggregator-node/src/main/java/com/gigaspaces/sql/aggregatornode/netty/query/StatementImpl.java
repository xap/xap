package com.gigaspaces.sql.aggregatornode.netty.query;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlValidator;

class StatementImpl implements Statement {
    private final QueryProviderImpl queryProvider;
    private final String name;
    private final SqlNode query;
    private final SqlValidator validator;
    private final StatementDescription description;

    public StatementImpl(QueryProviderImpl queryProvider, String name, SqlNode query, SqlValidator validator, StatementDescription description) {
        this.queryProvider = queryProvider;
        this.name = name;
        this.query = query;
        this.validator = validator;
        this.description = description;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public SqlNode getQuery() {
        return query;
    }

    @Override
    public SqlValidator getValidator() {
        return validator;
    }

    @Override
    public StatementDescription getDescription() {
        return description;
    }

    @Override
    public void close() {
        queryProvider.closeS(name);
    }
}
