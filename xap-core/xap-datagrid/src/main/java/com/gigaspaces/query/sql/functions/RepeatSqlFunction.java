package com.gigaspaces.query.sql.functions;

import java.util.Collections;

public class RepeatSqlFunction extends SqlFunction {
    @Override
    public Object apply(SqlFunctionExecutionContext context) {
        assertNumberOfArguments(2, context);
        Object text = context.getArgument(0);
        Object number = context.getArgument(1);
        if (!isString(text))
            throw new RuntimeException("Repeat function - 1st argument must be a String: " + text);
        if (!isWholeNumber(number))
            throw new RuntimeException("Repeat function - 2nd argument must be an Integer: " + number);
        if (((Number)number).intValue() == 0)
            return "";
        return String.join("", Collections.nCopies(((Number) number).intValue(), (String)text));
    }
}
