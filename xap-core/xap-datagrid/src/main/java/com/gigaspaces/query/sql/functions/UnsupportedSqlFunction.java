package com.gigaspaces.query.sql.functions;

/**
 * An implementation of a function that is not supported by
 * the backend.
 */
// TODO: It is better to track these functions at the planning time
//  and throw an exception before the execution of the query.
//  this would require integration with a visitor that converts
//  RelNode to a physical plan.
@com.gigaspaces.api.InternalApi
public class UnsupportedSqlFunction extends SqlFunction {
    private final String name;

    public UnsupportedSqlFunction(String name) {
        this.name = name;
    }

    @Override
    public Object apply(SqlFunctionExecutionContext context) {
        throw new UnsupportedOperationException(name + " is not supported.");
    }
}
