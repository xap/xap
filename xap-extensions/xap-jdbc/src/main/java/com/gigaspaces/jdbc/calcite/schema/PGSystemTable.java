package com.gigaspaces.jdbc.calcite.schema;

import com.gigaspaces.jdbc.calcite.schema.type.PgType;
import com.gigaspaces.jdbc.calcite.schema.type.TypeUtils;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.sql.type.SqlTypeName;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;

import static com.gigaspaces.jdbc.calcite.schema.GSSchemaTable.SchemaProperty;

public enum PGSystemTable {
    pg_am(
        "oid", "oid",
        "amname", "name",
        "amhandler", "regproc",
        "amtype", "char"
    ),
    pg_attribute(
        "attrelid", "oid",
        "attname", "name",
        "atttypid", "oid",
        "attstattarget", "int4",
        "attlen", "int2",
        "attnum", "int2",
        "attndims", "int4",
        "attcacheoff", "int4",
        "atttypmod", "int4",
        "attbyval", "bool",
        "attstorage", "char",
        "attalign", "char",
        "attnotnull", "bool",
        "atthasdef", "bool",
        "attisdropped", "bool",
        "attislocal", "bool",
        "attinhcount", "int4"
    ),
    pg_class(
        "oid", "oid",
        "relname", "name",
        "relnamespace", "oid",
        "reltype", "oid",
        "relowner", "oid",
        "relam", "oid",
        "relfilenode", "oid",
        "reltablespace", "oid",
        "relpages", "int4",
        "reltuples", "float4",
        "reltoastrelid", "oid",
        "relhasindex", "bool",
        "relisshared", "bool",
        "relkind", "char",
        "relnatts", "int2",
        "relchecks", "int2",
        "reltriggers", "int2",
        "relhasrules", "bool",
        "relhastriggers", "bool",
        "relhassubclass", "bool",
        "relacl", "aclitem_array",
        "reloptions", "text_array"
    ),
    pg_constraint(
        "oid", "oid",
        "conname", "name",
        "connamespace", "oid",
        "contype", "char",
        "condeferrable", "bool",
        "condeferred", "bool",
        "convalidated", "bool",
        "conrelid", "oid",
        "contypid", "oid",
        "conindid", "oid",
        "conparentid", "oid",
        "confrelid", "oid",
        "confupdtype", "oid",
        "confdeltype", "oid",
        "confmatchtype", "char",
        "conislocal", "char",
        "coninhcount", "char",
        "connoinherit", "bool",
        "conkey", "int2_array",
        "confkey", "int2_array",
        "conpfeqop", "oid_array",
        "conppeqop", "oid_array",
        "conffeqop", "oid_array",
        "conexclop", "oid_array",
        "conbin", "pg_node_tree"
    ),
    pg_database(
        "oid", "oid",
        "datname", "name",
        "datdba", "oid",
        "encoding", "int4",
        "datcollate", "name",
        "datctype", "name",
        "datistemplate", "bool",
        "datallowconn", "bool",
        "datconnlimit", "int4",
        "datlastsysoid", "oid",
        "datfrozenxid", "xid",
        "datminmxid", "xid",
        "dattablespace", "oid",
        "datacl", "aclitem_array"
    ),
    pg_index(
        "oid", "oid",
        "indexrelid", "oid",
        "indrelid", "oid",
        "indnatts", "int2",
        "indnkeyatts", "int2",
        "indisunique", "bool",
        "indisprimary", "bool",
        "indisexclusion", "bool",
        "indimmediate", "bool",
        "indisclustered", "bool",
        "indisvalid", "bool",
        "indcheckxmin", "bool",
        "indisready", "bool",
        "indislive", "bool",
        "indisreplident", "bool",
        "indkey", "int2vector",
        "indcollation", "oidvector",
        "indclass", "oidvector",
        "indoption", "int2vector",
        "indexprs", "pg_node_tree",
        "indpred", "pg_node_tree"
    ),
    pg_namespace(
        "oid", "oid",
        "nspname", "name",
        "nspowner", "oid",
        "nspacl", "aclitem_array"
    ),
    pg_proc(
        "oid", "oid",
        "proname", "name",
        "pronamespace", "oid",
        "proowner", "oid",
        "prolang", "oid",
        "procost", "float4",
        "prorows", "float4",
        "provariadic", "oid",
        "prosupport", "regproc",
        "prokind", "char",
        "prosecdef", "bool",
        "proleakproof", "bool",
        "proisstrict", "bool",
        "proretset", "bool",
        "provolatile", "char",
        "proparallel", "char",
        "pronargs", "int2",
        "pronargdefaults", "int2",
        "prorettype", "oid",
        "proargtypes", "oidvector",
        "proallargtypes", "oid_array",
        "proargmodes", "char_array",
        "proargnames", "text_array",
        "proargdefaults", "pg_node_tree",
        "protrftypes", "oid_array",
        "prosrc", "text",
        "probin", "text",
        "proconfig", "text_array",
        "proacl", "aclitem_array"
    ),
    pg_trigger(
        "oid", "oid",
        "tgrelid", "oid",
        "tgparentid", "oid",
        "tgname", "name",
        "tgfoid", "oid",
        "tgtype", "int2",
        "tgenabled", "char",
        "tgisinternal", "bool",
        "tgconstrrelid", "oid",
        "tgconstrindid", "oid",
        "tgconstraint", "oid",
        "tgdeferrable", "bool",
        "tginitdeferred", "bool",
        "tgnargs", "int2",
        "tgattr", "int2vector",
        "tgargs", "bytea",
        "tgqual", "pg_node_tree",
        "tgoldtable", "name",
        "tgnewtable", "name"
    ),
    pg_type(
        "oid", "oid",
        "typname", "name",
        "typnamespace", "oid",
        "typowner", "oid",
        "typlen", "int2",
        "typbyval", "bool",
        "typtype", "char",
        "typisdefined", "bool",
        "typdelim", "char",
        "typrelid", "oid",
        "typelem", "oid",
        "typinput", "regproc",
        "typoutput", "regproc",
        "typreceive", "regproc",
        "typsend", "regproc",
        "typanalyze", "regproc",
        "typalign", "char",
        "typstorage", "char",
        "typnotnull", "bool",
        "typbasetype", "oid",
        "typtypmod", "int4",
        "typndims", "int4",
        "typdefaultbin", "pg_node_tree",
        "typdefault", "text"
    );
    private final SchemaProperty[] properties;

    PGSystemTable(String... columns) {
        ArrayList<SchemaProperty> properties = new ArrayList<>();
        for (int i = 0; i < columns.length;) {
            String name = columns[i++];
            String typeName = columns[i++];
            PgType type = TypeUtils.typeByName(typeName);
            SqlTypeName sqlTypeName = TypeUtils.sqlTypeName(type);
            RelProtoDataType protoType = TypeUtils.protoType(type);
            properties.add(new SchemaProperty(name, sqlTypeName, protoType));
        }
        this.properties = properties.toArray(new SchemaProperty[0]);
    }

    public SchemaProperty[] getProperties() {
        return properties;
    }

    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        RelDataTypeFactory.Builder b = new RelDataTypeFactory.Builder(typeFactory);
        for (SchemaProperty p : properties) {
            b.add(p.getPropertyName(), p.getProtoDataType().apply(typeFactory));
        }
        return b.build();
    }
}
