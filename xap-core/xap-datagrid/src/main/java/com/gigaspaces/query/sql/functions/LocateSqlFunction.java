package com.gigaspaces.query.sql.functions;

@com.gigaspaces.api.InternalApi
public class LocateSqlFunction extends SqlFunction {
    @Override
    public Object apply(SqlFunctionExecutionContext context) {
        assertNumberOfArguments(2, context);
        Object toLocate = context.getArgument(0);
        Object str = context.getArgument(1);
        if (str != null && toLocate != null && str instanceof String && toLocate instanceof String) {
            return String.valueOf(str).indexOf(String.valueOf(toLocate)) + 1;
        } else {
            throw new RuntimeException("LOCATE function - wrong arguments types. First argument:[" + toLocate + "]. Second argument:[ " + str + "]");
        }
    }
}
