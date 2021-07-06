package com.gigaspaces.query.sql.functions;

import java.util.Collections;

/**
 * Concats a string to itself n times
 *
 * @author Tomer Shapira
 * @since 16.0.0
 */
public class RepeatSqlFunction extends SqlFunction {

    /**
     * @param context contains a string arguments to repeat, and an integer n that represents the times to repeats.
     * @return the string that results from concatenating the string to itself n times.
     */
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
