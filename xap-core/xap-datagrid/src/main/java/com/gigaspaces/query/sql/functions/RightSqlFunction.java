package com.gigaspaces.query.sql.functions;


import static java.lang.Math.abs;

/**
 * Returns a substring of a given string starting from the right
 *
 *
 * @author Tomer Shapira
 * @since 16.0.0
 */


public class RightSqlFunction extends SqlFunction{

    /**
     * @param context contains one String argument and one Integer argument.
     * @return if n > 0 the n last characters, else all characters but the first |n| charecters from t.
     */
    @Override
    public Object apply(SqlFunctionExecutionContext context) {
        assertNumberOfArguments(2, context);
        Object strObj = context.getArgument(0);
        Object numObj = context.getArgument(1);
        if (!(strObj instanceof String))
            throw new RuntimeException("Right function - 1st argument must be a String:" + strObj);
        if (!isWholeNumber(numObj))
            throw new RuntimeException("Right function - 2nd argument must be an Integer:" + numObj);
        String str = (String)strObj;
        int num = ((Number)numObj).intValue();
        if (num ==0){
            return "";
        }
        if (abs(num) > str.length()){
            return num < 0 ? "" : str;
        }
        return num > 0 ? str.substring(str.length()-num) : str.substring(abs(num));
    }
}
