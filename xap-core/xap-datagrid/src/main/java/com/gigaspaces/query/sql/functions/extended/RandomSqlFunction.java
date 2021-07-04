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
 * Built in mathematical sql function to perform random operation.
 *
 * @author Mishel Liberman
 * @since 16.0
 */
@com.gigaspaces.api.InternalApi
public class RandomSqlFunction extends SqlFunction {
    /**
     * @param context contains two arguments of either Long/Integer/Double.
     * @return the remainder of context.getArgument(0) divided by context.getArgument(1).
     */
    @Override
    public Object apply(SqlFunctionExecutionContext context) {
        assertNumberOfArguments(0, context);
        return Math.random();
    }
}
