package com.gigaspaces.query.sql.functions;


/**
 * Returns the position of the character where the first occurrence of the substring appears in a string.
 *
 *
 * @author Tomer Shapira
 * @since 16.0.0
 */



public class StrposFunction extends SqlFunction {
    /**
     * @param context contains two String arguments, a string and a substring.
     * @return index n where the first occurrence of the substring appears in a string.
     */
    @Override
    public Object apply(SqlFunctionExecutionContext context) {
        assertNumberOfArguments(2, context);
        Object str = context.getArgument(0);
        Object subStr = context.getArgument(1);
        if (!isString(str))
            throw new RuntimeException("Strpos function - 1st argument must be a String: " + str);
        if (!isString(subStr))
            throw new RuntimeException("Strpos function - 2nd argument must be a String: " + str);
        return ((String)str).indexOf((String) subStr)+1;
    }
}
