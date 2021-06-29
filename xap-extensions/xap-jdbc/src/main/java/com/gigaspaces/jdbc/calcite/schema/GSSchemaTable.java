package com.gigaspaces.jdbc.calcite.schema;

import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.metadata.PropertyInfo;
import com.gigaspaces.jdbc.calcite.schema.type.PgType;
import com.gigaspaces.jdbc.calcite.schema.type.TypeUtils;
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
import java.util.List;

public class GSSchemaTable extends AbstractTable {
    private final PGSystemTable systemTable;

    public GSSchemaTable(PGSystemTable systemTable) {
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
                for (String name : getSpaceTables(space)) {
                    String fqn = "public." + name;
                    int oid = IdGen.INSTANCE.oid(fqn);
                    ITypeDesc typeDesc = SQLUtil.checkTableExistence(name, space);
                    short idx = 0;
                    for (PropertyInfo property : typeDesc.getProperties()) {
                        SqlTypeName sqlTypeName = mapToSqlType(property.getType());
                        PgType pgType = TypeUtils.fromInternal(sqlTypeName);
                        result.addRow(new TableRow(queryColumns,
                                oid, // attrelid
                                property.getName(), // attname
                                pgType.getId(), // atttypid
                                0, // attstattarget
                                (short)pgType.getLength(), // attlen
                                ++idx,// attnum
                                (pgType.getElementType() != 0 ? 1 : 0), // attndims
                                -1, // attcacheoff
                                -1, // atttypmod
                                null, // attbyval
                                'p', // attstorage
                                'c', // attalign
                                false, // attnotnull
                                false, // atthasdef
                                false, // attisdropped
                                false, // attislocal
                                0// attinhcount
                        ));
                    }
                }

                break;
            }
            case pg_class: {
                for (String name : getSpaceTables(space)) {
                    String fqn = "public." + name;
                    int oid = IdGen.INSTANCE.oid(fqn);
                    ITypeDesc typeDesc = SQLUtil.checkTableExistence(name, space);
                    result.addRow(new TableRow(queryColumns,
                            oid,//    oid
                            name,//    relname
                            0,//    relnamespace
                            0,//    reltype
                            0,//    relowner
                            0,//    relam
                            0,//    relfilenode
                            0,//    reltablespace
                            0,//    relpages
                            100.0f,//    reltuples
                            0,//    reltoastrelid
                            false,//    relhasindex
                            false,//    relisshared
                            'r',//    relkind
                            (short) typeDesc.getProperties().length, //    relnatts
                            (short) 0,//    relchecks
                            (short) 0,//    reltriggers
                            false, //    relhasrules
                            false, //    relhastriggers
                            false, //    relhassubclass
                            null, // relacl
                            null//    reloptions
                    ));
                }

                break;
            }
            case pg_namespace: {
                result.addRow(new TableRow(queryColumns, 0, "PUBLIC", 0, null));
                result.addRow(new TableRow(queryColumns, -1000, "PG_CATALOG", 0, null));

                break;
            }
            case pg_type: {
                for (PgType type : TypeUtils.types()) {
                    TableRow row = new TableRow(queryColumns,
                            type.getId(), //            oid
                            type.getName(), //            typname
                            -1000, //            typnamespace
                            0,//            typowner
                            (short) type.getLength(),//            typlen
                            null,//            typbyval
                            'b',//            typtype
                            true, //            typisdefined
                            ',', //            typdelim
                            0, //            typrelid
                            type.getElementType() , //            typelem
                            0, //            typinput
                            0, //            typoutput
                            0, //            typreceive
                            0, //            typsend
                            0, //            typanalyze
                            'c', //            typalign
                            'p', //            typstorage
                            false, //            typnotnull
                            0, //            typbasetype
                            -1, //            typtypmod
                            (type.getElementType() != 0 ? 1 : 0), //            typndims
                            null, //            typdefaultbin
                            null//            typdefault
                    );

                    result.addRow(row);
                }

                break;
            }
            default:
                break; // will return empty result
        }
        return result;
    }

    private List<String> getSpaceTables(IJSpace space) throws SQLException {
        try {
            return ((IRemoteJSpaceAdmin) space.getAdmin()).getRuntimeInfo().m_ClassNames;
        } catch (RemoteException e) {
            throw new SQLException("Failed to get runtime info from space", e);
        }
    }

    private static SqlTypeName mapToSqlType(Class<?> clazz) {
        if (clazz == Short.class) {
            return SqlTypeName.SMALLINT;
        } else if (clazz == Integer.class) {
            return SqlTypeName.INTEGER;
        } else if (clazz == Long.class) {
            return SqlTypeName.BIGINT;
        } else if (clazz == Float.class) {
            return SqlTypeName.FLOAT;
        } else if (clazz == Double.class) {
            return SqlTypeName.DOUBLE;
        } else if (clazz == BigDecimal.class) {
            return SqlTypeName.DECIMAL;
        } else if (clazz == Boolean.class) {
            return SqlTypeName.BOOLEAN;
        } else if (clazz == String.class) {
            return SqlTypeName.VARCHAR;
        } else if (clazz == java.util.Date.class
                || clazz == java.sql.Date.class) {
            return SqlTypeName.DATE;
        } else if (clazz == java.sql.Time.class
                || clazz == java.time.Instant.class) {
            return SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE;
        } else if (clazz == java.sql.Timestamp.class) {
            return SqlTypeName.TIMESTAMP;
        } else if (clazz == java.time.LocalDateTime.class
                || clazz == java.time.LocalTime.class
                || clazz == java.time.LocalDate.class) {
            return SqlTypeName.TIME;
        }


        throw new UnsupportedOperationException("Unsupported type: " + clazz);
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