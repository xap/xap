package com.gigaspaces.query.sql.functions.extended;

import com.gigaspaces.query.sql.functions.SqlFunction;
import com.gigaspaces.query.sql.functions.SqlFunctionExecutionContext;

public class ChrSqlFunction extends SqlFunction {
    @Override
    public Object apply(SqlFunctionExecutionContext context) {
        assertNumberOfArguments(1, context);
        Object arg = context.getArgument(0);


        if (arg instanceof Integer || arg instanceof Character)
            return Character.toString((char) arg);
        throw new RuntimeException("Upper function - wrong argument type: " + arg);
    }
}
