package com.gigaspaces.query.sql.functions;

import static java.lang.Math.abs;

public class LeftSqlFunction extends SqlFunction{
    @Override
    public Object apply(SqlFunctionExecutionContext context) {
        assertNumberOfArguments(2, context);
        Object strObj = context.getArgument(0);
        Object numObj = context.getArgument(1);
        if (!isString(strObj))
            throw new RuntimeException("Left function - 1st argument must be a String:" + strObj);
        if (!isInteger(numObj))
            throw new RuntimeException("Left function - 2nd argument must be an Intger:" + numObj);
        String str = (String)strObj;
        int num = ((Number)numObj).intValue();
        if (num == 0){
            return "";
        }
        if (abs(num) > str.length()){
            return str;
        }
        return num > 0 ? str.substring(0, num) : str.substring(0, str.length()-abs(num));
    }
}
