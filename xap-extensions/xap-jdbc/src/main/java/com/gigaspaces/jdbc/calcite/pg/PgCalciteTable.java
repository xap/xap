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
package com.gigaspaces.jdbc.calcite.pg;

import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.metadata.PropertyInfo;
import com.gigaspaces.jdbc.calcite.GSTable;
import com.gigaspaces.jdbc.model.result.QueryResult;
import com.gigaspaces.jdbc.model.result.SchemaQueryResult;
import com.gigaspaces.jdbc.model.result.TableRow;
import com.gigaspaces.jdbc.model.table.IQueryColumn;
import com.gigaspaces.jdbc.model.table.SchemaTableContainer;
import com.j_spaces.core.IJSpace;
import com.j_spaces.core.admin.IRemoteJSpaceAdmin;
import com.j_spaces.jdbc.SQLUtil;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.math.BigDecimal;
import java.rmi.RemoteException;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PgCalciteTable extends AbstractTable {

    private static final Map<String, PgCalciteTable> TABLES = new LinkedHashMap<>();

    static {
        for (PgTable pgTable : PgTable.values()) {
            PgCalciteTable pgCalciteTable = new PgCalciteTable(pgTable);
            TABLES.put(pgCalciteTable.getName(), pgCalciteTable);
        }
    }

    public static PgCalciteTable getTable(String name) {
        return TABLES.get(name);
    }

    public static Set<String> getTableNames() {
        return TABLES.keySet();
    }

    private final PgTable systemTable;

    public PgCalciteTable(PgTable systemTable) {
        this.systemTable = systemTable;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return systemTable.getRowType(typeFactory);
    }

    public String getName() {
        return systemTable.name();
    }

    public SchemaProperty[] getSchemas() {
        return this.systemTable.getProperties();
    }

    public QueryResult execute(SchemaTableContainer schemaTableContainer, IJSpace space, List<IQueryColumn> tableColumns) throws SQLException {
        QueryResult result = new SchemaQueryResult(schemaTableContainer, tableColumns);
        IQueryColumn[] queryColumns = tableColumns.toArray(new IQueryColumn[0]);
        switch (systemTable) {
            case pg_attribute: {
                executePgAttribute(result, space, queryColumns);
                break;
            }
            case pg_class: {
                executePgClass(result, space, queryColumns);
                break;
            }
            case pg_namespace: {
                executePgNamespace(result, queryColumns);
                break;
            }
            case pg_type: {
                executePgType(result, queryColumns);
                break;
            }
            default:
                break; // will return empty result
        }
        return result;
    }

    private void executePgAttribute(QueryResult result, IJSpace space, IQueryColumn[] queryColumns) throws SQLException {
        for (String name : getSpaceTables(space)) {
            String fqn = "public." + name;
            int oid = PgOidGenerator.INSTANCE.oid(fqn);
            ITypeDesc typeDesc = SQLUtil.checkTableExistence(name, space);
            short idx = 0;
            for (PropertyInfo property : typeDesc.getProperties()) {
                SqlTypeName sqlTypeName = GSTable.mapToSqlType(property.getType());
                PgTypeDescriptor pgType = PgTypeUtils.fromSqlTypeName(sqlTypeName);
                result.addRow(new TableRow(queryColumns,
                        oid,                                    // attrelid
                        property.getName(),                     // attname
                        pgType.getId(),                         // atttypid
                        0,                                      // attstattarget
                        (short)pgType.getLength(),              // attlen
                        ++idx,                                  // attnum
                        (pgType.getElementType() != 0 ? 1 : 0), // attndims
                        -1,                                     // attcacheoff
                        -1,                                     // atttypmod
                        null,                                   // attbyval
                        'p',                                    // attstorage
                        'c',                                    // attalign
                        false,                                  // attnotnull
                        false,                                  // atthasdef
                        ' ',                                    // attidentity
                        false,                                  // attisdropped
                        false,                                  // attislocal
                        0                                       // attinhcount
                ));
            }
        }
    }

    private void executePgClass(QueryResult result, IJSpace space, IQueryColumn[] queryColumns) throws SQLException {
        for (String name : getSpaceTables(space)) {
            String fqn = "public." + name;
            int oid = PgOidGenerator.INSTANCE.oid(fqn);
            ITypeDesc typeDesc = SQLUtil.checkTableExistence(name, space);
            result.addRow(new TableRow(queryColumns,
                    oid,                                     // oid
                    name,                                    // relname
                    0,                                       // relnamespace
                    0,                                       // reltype
                    0,                                       // relowner
                    0,                                       // relam
                    0,                                       // relfilenode
                    0,                                       // reltablespace
                    0,                                       // relpages
                    100.0f,                                  // reltuples
                    0,                                       // reltoastrelid
                    false,                                   // relhasindex
                    false,                                   // relisshared
                    'r',                                     // relkind
                    (short) typeDesc.getProperties().length, // relnatts
                    (short) 0,                               // relchecks
                    (short) 0,                               // reltriggers
                    false,                                   // relhasrules
                    false,                                   // relhastriggers
                    false,                                   // relhassubclass
                    null,                                    // relacl
                    null                                     // reloptions
            ));
        }
    }

    private void executePgNamespace(QueryResult result, IQueryColumn[] queryColumns) {
        result.addRow(new TableRow(queryColumns, 0, "PUBLIC", 0, null));
        result.addRow(new TableRow(queryColumns, -1000, "PG_CATALOG", 0, null));
    }

    private void executePgType(QueryResult result, IQueryColumn[] queryColumns) {
        for (PgTypeDescriptor type : PgTypeUtils.getTypes()) {
            TableRow row = new TableRow(queryColumns,
                    type.getId(),                         // oid
                    type.getName(),                       // typname
                    -1000,                                // typnamespace
                    0,                                    // typowner
                    (short) type.getLength(),             // typlen
                    null,                                 // typbyval
                    'b',                                  // typtype
                    true,                                 // typisdefined
                    ',',                                  // typdelim
                    0,                                    // typrelid
                    type.getElementType() ,               // typelem
                    0,                                    // typinput
                    0,                                    // typoutput
                    0,                                    // typreceive
                    0,                                    // typsend
                    0,                                    // typanalyze
                    'c',                                  // typalign
                    'p',                                  // typstorage
                    false,                                // typnotnull
                    0,                                    // typbasetype
                    -1,                                   // typtypmod
                    (type.getElementType() != 0 ? 1 : 0), // typndims
                    null,                                 // typdefaultbin
                    null                                  // typdefault
            );

            result.addRow(row);
        }
    }

    private List<String> getSpaceTables(IJSpace space) throws SQLException {
        try {
            return ((IRemoteJSpaceAdmin) space.getAdmin()).getRuntimeInfo().m_ClassNames;
        } catch (RemoteException e) {
            throw new SQLException("Failed to get runtime info from space", e);
        }
    }

    public static class SchemaProperty {
        private final String name;
        private final SqlTypeName typeName;
        private final RelProtoDataType protoType;

        public SchemaProperty(String name, SqlTypeName typeName, RelProtoDataType protoType) {
            this.name = name;
            this.typeName = typeName;
            this.protoType = protoType;
        }

        public String getPropertyName() {
            return name;
        }

        public RelProtoDataType getProtoDataType() {
            return protoType;
        }

        public Class<?> getJavaType() {
            switch (typeName) {
                case VARCHAR:
                    return String.class;
                case CHAR:
                    return Character.class;
                case DATE:
                    return Date.class;
                case TIME:
                    return Time.class;
                case TIME_WITH_LOCAL_TIME_ZONE:
                    return LocalTime.class;
                case INTEGER:
                    return Integer.class;
                case TIMESTAMP:
                    return Timestamp.class;
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                    return LocalDateTime.class;
                case BIGINT:
                    return Long.class;
                case SMALLINT:
                    return Short.class;
                case TINYINT:
                    return Byte.class;
                case DECIMAL:
                    return BigDecimal.class;
                case BOOLEAN:
                    return Boolean.class;
                case DOUBLE:
                    return Double.class;
                case REAL:
                case FLOAT:
                    return Float.class;
                case ANY:
                    return Object.class;
                case NULL:
                    return Void.class;
            }
            return null;
        }
    }
}