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

package com.gigaspaces.query.sql.functions.extended;

import com.gigaspaces.query.sql.functions.SqlFunction;
import com.gigaspaces.query.sql.functions.SqlFunctionExecutionContext;

/**
 * Built in mathematical sql function to perform power operation.
 *
 * @author Mishel Liberman
 * @since 16.0
 */
@com.gigaspaces.api.InternalApi
public class PowerSqlFunction extends SqlFunction {

    /**
     * @param context contains two arguments of Number.
     * @return the result of context.getArgument0(0) power context.getArgument(1).
     */
    @Override
    public Object apply(SqlFunctionExecutionContext context) {
        assertNumberOfArguments(2, context);

        Object num = context.getArgument(0);
        Object power = context.getArgument(1);
        if (!(num instanceof Number) || !(power instanceof Number)) {
            throw new RuntimeException("Mod function - wrong arguments types, both arguments should be Number. First argument:[" + num + "]. Second argument:" + power + "].");
        }
        return Math.pow(((Number) num).doubleValue(), ((Number) power).doubleValue());
    }
}
