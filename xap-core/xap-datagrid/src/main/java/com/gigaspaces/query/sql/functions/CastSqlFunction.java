/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
