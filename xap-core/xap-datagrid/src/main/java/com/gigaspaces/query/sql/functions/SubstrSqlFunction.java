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
 * Returns a substring of a given string
 *
 * @author Tomer Shapira
 * @since 16.0.0
 */
public class SubstrSqlFunction extends SqlFunction {
    /**
     * @param context contains one String argument and one Integer argument n. optionally contains another Integer argument m.
     * @return a substring staring from the n character of the string, if m is given returns m characters starting from n.
     */
    @Override
    public Object apply(SqlFunctionExecutionContext context) {
        assertNumberOfArguments(2, 3, context);
        Object textObj = context.getArgument(0);
        if (!isString(textObj)) {
            throw new RuntimeException("Substr function - 1st argument must be a String");
        }
        String text = (String) textObj;
        Object startObj = context.getArgument(1);
        if (!isWholeNumber(startObj)) {
            throw new RuntimeException("Substr function - 2nd argument must be an Intger:" + startObj);
        }
        int start = ((Number) startObj).intValue() - 1;
        Object countObj = null;
        if (context.getNumberOfArguments() == 3) {
            countObj = context.getArgument(2);
            if (!isWholeNumber(countObj)) {
                throw new RuntimeException("Substr function - 3rd argument must be an Intger:" + countObj);
            }
            int count = ((Number) countObj).intValue();
            if (count < 0) {
                throw new RuntimeException("Substr function - negative substring length not allowed:" + count);
            }
            if (start < 0) {
                if (start + count <= 0) {
                    return "";
                }
                return text.substring(0, start + count);
            }
            if (start + count > text.length()) {
                return text.substring(start);
            }
            return text.substring(start, start + count);
        }
        if (start <= 0) {
            return text;
        }
        if (start > text.length()) {
            return "";
        }
        return text.substring(start);
    }
}
