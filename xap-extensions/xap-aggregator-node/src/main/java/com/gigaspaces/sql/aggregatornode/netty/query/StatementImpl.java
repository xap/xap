package com.gigaspaces.sql.aggregatornode.netty.query;

import com.gigaspaces.jdbc.calcite.GSOptimizer;
import org.apache.calcite.sql.SqlNode;

class StatementImpl implements Statement {
    private final QueryProviderImpl queryProvider;
    private final String name;
    private final SqlNode query;
    private final GSOptimizer optimizer;
    private final StatementDescription description;

    public StatementImpl(QueryProviderImpl queryProvider, String name, SqlNode query, GSOptimizer optimizer, StatementDescription description) {
        this.queryProvider = queryProvider;
        this.name = name;
        this.query = query;
        this.optimizer = optimizer;
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
    public GSOptimizer getOptimizer() {
        return optimizer;
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
