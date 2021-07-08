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
