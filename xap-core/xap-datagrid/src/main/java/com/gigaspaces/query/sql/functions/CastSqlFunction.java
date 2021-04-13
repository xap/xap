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

import com.gigaspaces.internal.utils.math.MutableNumber;

public class CastSqlFunction extends SqlFunction {
    @Override
    public Object apply(SqlFunctionExecutionContext context) {
        assertNumberOfArguments(2, context);
        Object source = context.getArgument(0);
        Class target = (Class) context.getArgument(1);

        return get(source, target);
    }

    private Object get(Object val, Class className) {
        MutableNumber mutableNumber = MutableNumber.fromClass(val.getClass(), false);
        mutableNumber.add((Number) val);
        return mutableNumber.cast(className);
    }
}
