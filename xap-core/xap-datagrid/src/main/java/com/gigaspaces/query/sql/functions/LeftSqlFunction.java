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

import static java.lang.Math.abs;

/**
 * Returns a substring of a given string starting from the left
 *
 *
 * @author Tomer Shapira
 * @since 16.0.0
 */

public class LeftSqlFunction extends SqlFunction{
    /**
     * @param context contains one String argument and one Integer argument.
     * @return if n > 0 the n first characters, else all characters but the last |n| charecters from t.
     */
    @Override
    public Object apply(SqlFunctionExecutionContext context) {
        assertNumberOfArguments(2, context);
        Object strObj = context.getArgument(0);
        Object numObj = context.getArgument(1);
        if (!isString(strObj))
            throw new RuntimeException("Left function - 1st argument must be a String:" + strObj);
        if (!isWholeNumber(numObj))
            throw new RuntimeException("Left function - 2nd argument must be an Intger:" + numObj);
        String str = (String)strObj;
        int num = ((Number)numObj).intValue();
        if (num == 0){
            return "";
        }
        if (abs(num) > str.length()){
            return num < 0 ? "" : str;
        }
        return num > 0 ? str.substring(0, num) : str.substring(0, str.length()-abs(num));
    }
}
