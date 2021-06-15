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
package com.gigaspaces.jdbc.calcite;

import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.metadata.PropertyInfo;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

public class GSTable extends AbstractTable {

    private final ITypeDesc typeDesc;
    private final String name;

    public GSTable(String name, ITypeDesc typeDesc) {
        this.name = name;
        this.typeDesc = typeDesc;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
        for (PropertyInfo property : typeDesc.getProperties()) {
            builder.add(
                property.getName(),
                mapToSqlType(property.getType())
            );
        }
        return builder.build();
    }

    private static SqlTypeName mapToSqlType(Class<?> clazz) {
        if (clazz == Integer.class) {
            return SqlTypeName.INTEGER;
        } else if (clazz == Long.class) {
            return SqlTypeName.BIGINT;
        } else if (clazz == String.class) {
            return SqlTypeName.VARCHAR;
        } else if (clazz == java.util.Date.class) {
            return SqlTypeName.DATE;
        } else if (clazz == java.sql.Time.class) {
            return SqlTypeName.TIME;
        } else if (clazz == java.sql.Timestamp.class) {
            return SqlTypeName.TIMESTAMP;
        }


        throw new UnsupportedOperationException("Unsupported type: " + clazz);
    }

    public String getName() {
        return name;
    }
}
