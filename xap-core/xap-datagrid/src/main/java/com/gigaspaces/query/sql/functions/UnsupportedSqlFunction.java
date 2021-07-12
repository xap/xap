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
 * An implementation of a function that is not supported by
 * the backend.
 */
// TODO: It is better to track these functions at the planning time
//  and throw an exception before the execution of the query.
//  this would require integration with a visitor that converts
//  RelNode to a physical plan.
@com.gigaspaces.api.InternalApi
public class UnsupportedSqlFunction extends SqlFunction {
    private final String name;

    public UnsupportedSqlFunction(String name) {
        this.name = name;
    }

    @Override
    public Object apply(SqlFunctionExecutionContext context) {
        throw new UnsupportedOperationException(name + " is not supported.");
    }
}
