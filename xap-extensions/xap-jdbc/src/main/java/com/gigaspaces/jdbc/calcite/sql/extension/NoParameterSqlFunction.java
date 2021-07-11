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
package com.gigaspaces.jdbc.calcite.sql.extension;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.fun.SqlAbstractTimeFunction;
import org.apache.calcite.sql.type.SqlTypeName;

public class NoParameterSqlFunction extends SqlAbstractTimeFunction {

    private final SqlTypeName returnTypeName;

    public NoParameterSqlFunction(
            String functionName, SqlTypeName returnTypeName) {
        // access protected constructor
        super(functionName, returnTypeName);
        this.returnTypeName = returnTypeName;
    }

    @Override
    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
        return opBinding.getTypeFactory().createSqlType(returnTypeName);
    }

    @Override
    public boolean isDeterministic() {
        return false;
    }
}