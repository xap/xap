package com.gigaspaces.query.sql.functions;

import com.gigaspaces.internal.utils.math.MutableNumber;

public class CastSqlFunction extends SqlFunction {
    @Override
    public Object apply(SqlFunctionExecutionContext context) {
        assertNumberOfArguments(2, context);
        Object source = context.getArgument(0);
        Class target = (Class) context.getArgument(1);

        return get(source, target);
    }

    private Object get(Object val, Class className) {
        MutableNumber mutableNumber = MutableNumber.fromClass(val.getClass(), false);
        mutableNumber.add((Number) val);
        return mutableNumber.cast(className);
    }
}
