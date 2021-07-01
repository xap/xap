package com.gigaspaces.query.sql.functions;

import com.gigaspaces.internal.utils.ObjectConverter;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class CastSqlFunction extends SqlFunction {

    private static final Map<String, Class<?>> types = new HashMap<>();

    static {
        types.put("DOUBLE", Double.TYPE);
        types.put("FLOAT", Float.TYPE);
        types.put("INTEGER", Integer.TYPE);
//        types.put("LONG", Long.TYPE);
//        types.put("Short", Short.TYPE);
    }

    @Override
    public Object apply(SqlFunctionExecutionContext context) {
        assertNumberOfArguments(1, context);
        String type = ((SqlFunctionExecutionContextWithType)context).getType();
        Object value = context.getArgument(0);
        try {
           return ObjectConverter.convert(value, types.get(type));
        }
        catch (SQLException throwable) {
            throw new RuntimeException("Cast function - Invalid input syntax for " + value);
        }
    }
}
