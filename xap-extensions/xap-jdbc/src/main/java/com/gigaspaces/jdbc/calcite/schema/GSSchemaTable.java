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
package com.gigaspaces.jdbc.calcite.schema;

import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.jdbc.model.result.QueryResult;
import com.gigaspaces.jdbc.model.result.TableRow;
import com.gigaspaces.jdbc.model.table.QueryColumn;
import com.j_spaces.core.IJSpace;
import com.j_spaces.core.admin.IRemoteJSpaceAdmin;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.List;

public class GSSchemaTable extends AbstractTable {
    private final PGSystemTable systemTable;

    public GSSchemaTable(PGSystemTable systemTable) {
        this.systemTable = systemTable;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
        for (SchemaProperty schema : this.systemTable.getProperties()) {
            builder.add(schema.getPropertyName(), schema.getSqlTypeName());
        }
        return builder.build();
    }

    public String getName() {
        return systemTable.name();
    }

    public SchemaProperty[] getSchemas() {
        return this.systemTable.getProperties();
    }

    public QueryResult execute(IJSpace space, List<QueryColumn> queryColumns) throws SQLException {
        QueryResult queryResult = new QueryResult(queryColumns);
        QueryColumn[] arr = queryColumns.toArray(new QueryColumn[0]);
        switch (systemTable) {
            case pg_am:
            case pg_attrdef:
                break; // will return empty result
            case pg_tables:
                getSpaceTables(space).forEach(table -> {
                    ITypeDesc typeDesc = space.getDirectProxy().getTypeManager().getTypeDescByName(table);
                    queryResult.add(new TableRow(arr, table, !typeDesc.getIndexes().isEmpty()));
                });
            default:
                throw new UnsupportedOperationException("Unhandled system table " + systemTable.name());
        }
        return queryResult;
    }

    private List<String> getSpaceTables(IJSpace space) throws SQLException {
        try {
            return ((IRemoteJSpaceAdmin) space.getAdmin()).getRuntimeInfo().m_ClassNames;
        } catch (RemoteException e) {
            throw new SQLException("Failed to get runtime info from space", e);
        }
    }


    public static class SchemaProperty {

        private final String propertyName;
        private final SqlTypeName sqlTypeName;

        public SchemaProperty(String propertyName, SqlTypeName sqlTypeName) {
            this.propertyName = propertyName;
            this.sqlTypeName = sqlTypeName;
        }

        public String getPropertyName() {
            return propertyName;
        }

        public SqlTypeName getSqlTypeName() {
            return sqlTypeName;
        }

        public static SchemaProperty of(String propertyName, SqlTypeName typeName) {
            return new SchemaProperty(propertyName, typeName);
        }
    }
}
