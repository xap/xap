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

import java.util.Map;

/**
 * Built in sql function to check if a map field contains some key
 *
 * @author Yael Nahon
 * @since 15.0.0
 */
@com.gigaspaces.api.InternalApi
public class ContainsKeySqlFunction extends SqlFunction {
    /***
     *
     * @param context contains the arguments, {@link com.gigaspaces.query.sql.functions.SqlFunctionExecutionContext}
     * @return true if the map contains a the key and false otherwise
     */
    @Override
    public Object apply(SqlFunctionExecutionContext context) {
        assertNumberOfArguments(2, context);
        Object map = context.getArgument(0);
        Object keyToCheck = context.getArgument(1);
        if (map instanceof Map && keyToCheck != null) {
            return ((Map) map).keySet().contains(keyToCheck);
        } else {
            throw new RuntimeException("containsKey function - wrong arguments types. First argument:[" + map + "]. Second argument:[ " + keyToCheck + "]");
        }
    }
}
