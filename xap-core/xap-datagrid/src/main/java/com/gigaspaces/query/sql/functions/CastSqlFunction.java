package com.gigaspaces.query.sql.functions;

import com.gigaspaces.internal.utils.ObjectConverter;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

/**
 * Casts a String into the wanted data object
 *
 * @author Tomer Shapira
 * @since 16.0.0
 */

public class CastSqlFunction extends SqlFunction {

    private static final Map<String, Class<?>> types = new HashMap<>();

    static {
        types.put("DOUBLE", Double.TYPE);
        types.put("FLOAT", Float.TYPE);
        types.put("INTEGER", Integer.TYPE);
        types.put("LONG", Long.TYPE);
        types.put("Short", Short.TYPE);
        types.put("TIMESTAMP", Timestamp.class);
        types.put("DATE", LocalDateTime.class);
        types.put("TIME", LocalDateTime.class);
        types.put("TIMESTAMP_WITH_LOCAL_TIME_ZONE", Instant.class);
    }

    /**
     * @param context contains one String argument and the type to cast to.
     * @return object of the wanted type.
     */
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
