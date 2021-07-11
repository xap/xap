package com.gigaspaces.query.sql.functions;

/**
 * A system function that returns a decompiled source
 * code of a Postgres expression. We do not support
 * that, so just returning an empty string.
 * <p>
 * See https://www.postgresql.org/docs/13/functions-info.html, Table 9.68.
 */
@com.gigaspaces.api.InternalApi
public class PgGetExprSqlFunction extends SqlFunction {
    @Override
    public Object apply(SqlFunctionExecutionContext context) {
        return "";
    }
}
