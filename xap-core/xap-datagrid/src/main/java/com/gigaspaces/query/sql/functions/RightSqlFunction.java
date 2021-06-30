package com.gigaspaces.query.sql.functions;


import static java.lang.Math.abs;

public class RightSqlFunction extends SqlFunction{
    @Override
    public Object apply(SqlFunctionExecutionContext context) {
        assertNumberOfArguments(2, context);
        Object strObj = context.getArgument(0);
        Object numObj = context.getArgument(1);
        if (!(strObj instanceof String))
            throw new RuntimeException("Right function - 1st argument must be a String:" + strObj);
        if (!isInteger(numObj))
            throw new RuntimeException("Right function - 2nd argument must be an Integer:" + numObj);
        String str = (String)strObj;
        int num = ((Number)numObj).intValue();
        if (num ==0){
            return "";
        }
        if (abs(num) > str.length()){
            return str;
        }
        return num > 0 ? str.substring(str.length()-num) : str.substring(abs(num));
    }
}
