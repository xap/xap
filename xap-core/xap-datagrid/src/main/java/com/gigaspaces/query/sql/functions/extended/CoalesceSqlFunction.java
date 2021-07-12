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
 * The COALESCE function returns the first of its arguments that is not null.
 * Null is returned only if all arguments are null.
 * It is often used to substitute a default value for null values when data is retrieved for display
 */
public class CoalesceSqlFunction extends SqlFunction {

    @Override
    public Object apply(SqlFunctionExecutionContext context) {
        if (context.getNumberOfArguments() == 0) {
            throw new RuntimeException("wrong number of arguments - actual number of arguments is: " + context.getNumberOfArguments());
        }

        for (int i = 0; i < context.getNumberOfArguments(); i++) {
            Object o = context.getArgument(i);
            ;
            if (o != null) return o;
        }
        return null;
    }
}
