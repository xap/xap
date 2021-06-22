package com.gigaspaces.query.sql.functions.extended;

import com.gigaspaces.query.sql.functions.SqlFunction;
import com.gigaspaces.query.sql.functions.SqlFunctionExecutionContext;

/**
 * The COALESCE function returns the first of its arguments that is not null.
 * Null is returned only if all arguments are null.
 * It is often used to substitute a default value for null values when data is retrieved for display
 */
public class CoalesceSqlFunction extends SqlFunction {

    @Override
    public Object apply(SqlFunctionExecutionContext context) {
        if (context.getNumberOfArguments() == 0) {
            throw new RuntimeException("wrong number of arguments - actual number of arguments is: " + context.getNumberOfArguments());
        }

        for (int i = 0; i < context.getNumberOfArguments(); i++) {
            Object o = context.getArgument(i);
            ;
            if (o != null) return o;
        }
        return null;
    }
}
