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
package com.gigaspaces.jdbc.model.table;

import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.query.sql.functions.SqlFunction;
import com.gigaspaces.query.sql.functions.SqlFunctionExecutionContextWithType;
import com.j_spaces.jdbc.SQLFunctions;

import java.util.List;

public class CastFunctionCallColumn extends FunctionCallColumn{
    private final String type;

    public CastFunctionCallColumn(List<IQueryColumn> params, String columnName, String functionName, String columnAlias, boolean isVisible, int columnOrdinal, String type) {
        super(params, functionName, columnName, columnAlias, isVisible, columnOrdinal);
        this.type = type;
    }

    public String getType() {
        return type;
    }

    @Override
    public Object getValue(IEntryPacket entryPacket) {
        SqlFunction sqlFunction = SQLFunctions.getBuiltInFunction(getFunctionName());
        if(sqlFunction != null){
            return sqlFunction.apply(new SqlFunctionExecutionContextWithType() {
                @Override
                public int getNumberOfArguments() {
                    return params.size();
                }

                @Override
                public Object getArgument(int index) {
                    return params.get(index).getValue(entryPacket);
                }

                @Override
                public String getType(){
                    return type;
                }
            });
        }
        throw new RuntimeException("Unknown function [" + getFunctionName() + "]");
    }
}
