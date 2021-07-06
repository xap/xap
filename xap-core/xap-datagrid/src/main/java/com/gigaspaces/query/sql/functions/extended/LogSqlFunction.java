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
 * Built in mathematical sql function to perform log operation.
 *
 * @author Mishel Liberman
 * @since 16.0
 */
@com.gigaspaces.api.InternalApi
public class LogSqlFunction extends SqlFunction {
    /**
     * @param context contains either one or two arguments of Number.
     * @return the result of log of context.getArgument0(0) divided by context.getArgument(1) or log 10 of context.getArgument0(0).
     */
    @Override
    public Object apply(SqlFunctionExecutionContext context) {
        assertNumberOfArguments(1, 2, context);

        if (context.getNumberOfArguments() == 1) {
            Object logNumber = context.getArgument(0);
            if (!(logNumber instanceof Number)) {
                throw new RuntimeException("Mod function - wrong arguments types, arguments should be Number. First argument:[" + logNumber + "]");
            }
            return Math.log10(((Number) logNumber).doubleValue());
        } else {
            //context.getNumberOfArguments() == 2
            Object base = context.getArgument(0);
            Object logNumber = context.getArgument(1);
            if (!(base instanceof Number) || !(logNumber instanceof Number)) {
                throw new RuntimeException("Mod function - wrong arguments types, both arguments should be Number. First argument:[" + base + "]. Second argument:[ " + logNumber + "]");
            }
            return Math.log(((Number) logNumber).doubleValue()) / Math.log(((Number) base).doubleValue());
        }
    }
}
