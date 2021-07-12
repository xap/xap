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
