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
 * Defines a routine, to be use in SQLQuery context, that accepts arguments and performs an action
 * on the stored object, such as a complex calculation.
 *
 * @author Barak Bar Orion
 * @since 11.0
 */
public abstract class SqlFunction {
    /**
     * Defines the action to be performed on the stored object.
     *
     * @param context contains the arguments, {@link com.gigaspaces.query.sql.functions.SqlFunctionExecutionContext}
     * @return The Stored object after applying the action
     * @throws RuntimeException if wrong input arguments were supplied
     */
    public abstract Object apply(SqlFunctionExecutionContext context);

    /**
     * Validate the number of arguments a function receives.
     *
     * @param expected number of arguments, according to every function inner logic.
     * @param context  the actual received arguments, which are contained in the context.
     * @throws RuntimeException if expected doesn't match actual number of arguments in context
     */
    protected void assertNumberOfArguments(int expected, SqlFunctionExecutionContext context) {
        if (context.getNumberOfArguments() != expected) {
            throw new RuntimeException("wrong number of arguments - expected: " + expected + " ,but actual number of arguments is: " + context.getNumberOfArguments());
        }
    }

    /**
     * Validate the number of arguments a function receives is in a given range.
     *
     * @param minExpected minimum number of arguments, according to every function inner logic.
     * @param maxExpected maximum number of arguments, according to every function inner logic.
     * @param context  the actual received arguments, which are contained in the context.
     * @throws RuntimeException if expected doesn't match actual number of arguments in context
     */
    protected void assertNumberOfArguments(int minExpected, int maxExpected, SqlFunctionExecutionContext context) {
        if (context.getNumberOfArguments() < minExpected || context.getNumberOfArguments() > maxExpected) {
            throw new RuntimeException("wrong number of arguments - expected between: " + minExpected + " and " + maxExpected + " ,but actual number of arguments is: " + context.getNumberOfArguments());
        }
    }

    protected boolean isWholeNumber(Object object){
        return object instanceof Integer || object instanceof Long || object instanceof Short;
    }

    protected boolean isString(Object object){
        return object instanceof String;
    }


}
