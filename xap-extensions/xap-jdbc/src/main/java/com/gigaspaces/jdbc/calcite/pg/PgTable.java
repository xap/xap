package com.gigaspaces.jdbc.calcite.pg;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;

import static com.gigaspaces.jdbc.calcite.pg.PgCalciteTable.SchemaProperty;

public enum PgTable {
    pg_am(
        column("oid", PgType.OID),
        column("amname", PgType.NAME),
        column("amhandler", PgType.REGPROC),
        column("amtype", PgType.CHAR)
    ),
    pg_attribute(
        column("attrelid", PgType.OID),
        column("attname", PgType.NAME),
        column("atttypid", PgType.OID),
        column("attstattarget", PgType.INT4),
        column("attlen", PgType.INT2),
        column("attnum", PgType.INT2),
        column("attndims", PgType.INT4),
        column("attcacheoff", PgType.INT4),
        column("atttypmod", PgType.INT4),
        column("attbyval", PgType.BOOL),
        column("attstorage", PgType.CHAR),
        column("attalign", PgType.CHAR),
        column("attnotnull", PgType.BOOL),
        column("atthasdef", PgType.BOOL),
        column("attisdropped", PgType.BOOL),
        column("attislocal", PgType.BOOL),
        column("attinhcount", PgType.INT4)
    ),
    pg_class(
        column("oid", PgType.OID),
        column("relname", PgType.NAME),
        column("relnamespace", PgType.OID),
        column("reltype", PgType.OID),
        column("relowner", PgType.OID),
        column("relam", PgType.OID),
        column("relfilenode", PgType.OID),
        column("reltablespace", PgType.OID),
        column("relpages", PgType.INT4),
        column("reltuples", PgType.FLOAT4),
        column("reltoastrelid", PgType.OID),
        column("relhasindex", PgType.BOOL),
        column("relisshared", PgType.BOOL),
        column("relkind", PgType.CHAR),
        column("relnatts", PgType.INT2),
        column("relchecks", PgType.INT2),
        column("reltriggers", PgType.INT2),
        column("relhasrules", PgType.BOOL),
        column("relhastriggers", PgType.BOOL),
        column("relhassubclass", PgType.BOOL),
        column("relacl", PgType.UNKNOWN),
        column("reloptions", PgType.TEXT.asArray())
    ),
    pg_constraint(
        column("oid", PgType.OID),
        column("conname", PgType.NAME),
        column("connamespace", PgType.OID),
        column("contype", PgType.CHAR),
        column("condeferrable", PgType.BOOL),
        column("condeferred", PgType.BOOL),
        column("convalidated", PgType.BOOL),
        column("conrelid", PgType.OID),
        column("contypid", PgType.OID),
        column("conindid", PgType.OID),
        column("conparentid", PgType.OID),
        column("confrelid", PgType.OID),
        column("confupdtype", PgType.OID),
        column("confdeltype", PgType.OID),
        column("confmatchtype", PgType.CHAR),
        column("conislocal", PgType.CHAR),
        column("coninhcount", PgType.CHAR),
        column("connoinherit", PgType.BOOL),
        column("conkey", PgType.INT2.asArray()),
        column("confkey", PgType.INT2.asArray()),
        column("conpfeqop", PgType.OID.asArray()),
        column("conppeqop", PgType.OID.asArray()),
        column("conffeqop", PgType.OID.asArray()),
        column("conexclop", PgType.OID.asArray()),
        column("conbin", PgType.NODE_TREE)
    ),
    pg_database(
        column("oid", PgType.OID),
        column("datname", PgType.NAME),
        column("datdba", PgType.OID),
        column("encoding", PgType.INT4),
        column("datcollate", PgType.NAME),
        column("datctype", PgType.NAME),
        column("datistemplate", PgType.BOOL),
        column("datallowconn", PgType.BOOL),
        column("datconnlimit", PgType.INT4),
        column("datlastsysoid", PgType.OID),
        column("datfrozenxid", PgType.UNKNOWN),
        column("datminmxid", PgType.UNKNOWN),
        column("dattablespace", PgType.OID),
        column("datacl", PgType.UNKNOWN)
    ),
    pg_index(
        column("oid", PgType.OID),
        column("indexrelid", PgType.OID),
        column("indrelid", PgType.OID),
        column("indnatts", PgType.INT2),
        column("indnkeyatts", PgType.INT2),
        column("indisunique", PgType.BOOL),
        column("indisprimary", PgType.BOOL),
        column("indisexclusion", PgType.BOOL),
        column("indimmediate", PgType.BOOL),
        column("indisclustered", PgType.BOOL),
        column("indisvalid", PgType.BOOL),
        column("indcheckxmin", PgType.BOOL),
        column("indisready", PgType.BOOL),
        column("indislive", PgType.BOOL),
        column("indisreplident", PgType.BOOL),
        column("indkey", PgType.INT2VECTOR),
        column("indcollation", PgType.OID_VECTOR),
        column("indclass", PgType.OID_VECTOR),
        column("indoption", PgType.INT2VECTOR),
        column("indexprs", PgType.NODE_TREE),
        column("indpred", PgType.NODE_TREE)
    ),
    pg_namespace(
        column("oid", PgType.OID),
        column("nspname", PgType.NAME),
        column("nspowner", PgType.OID),
        column("nspacl", PgType.UNKNOWN)
    ),
    pg_proc(
        column("oid", PgType.OID),
        column("proname", PgType.NAME),
        column("pronamespace", PgType.OID),
        column("proowner", PgType.OID),
        column("prolang", PgType.OID),
        column("procost", PgType.FLOAT4),
        column("prorows", PgType.FLOAT4),
        column("provariadic", PgType.OID),
        column("prosupport", PgType.REGPROC),
        column("prokind", PgType.CHAR),
        column("prosecdef", PgType.BOOL),
        column("proleakproof", PgType.BOOL),
        column("proisstrict", PgType.BOOL),
        column("proretset", PgType.BOOL),
        column("provolatile", PgType.CHAR),
        column("proparallel", PgType.CHAR),
        column("pronargs", PgType.INT2),
        column("pronargdefaults", PgType.INT2),
        column("prorettype", PgType.OID),
        column("proargtypes", PgType.OID_VECTOR),
        column("proallargtypes", PgType.OID.asArray()),
        column("proargmodes", PgType.CHAR.asArray()),
        column("proargnames", PgType.TEXT.asArray()),
        column("proargdefaults", PgType.NODE_TREE),
        column("protrftypes", PgType.OID.asArray()),
        column("prosrc", PgType.TEXT),
        column("probin", PgType.TEXT),
        column("proconfig", PgType.TEXT.asArray()),
        column("proacl", PgType.UNKNOWN)
    ),
    pg_trigger(
        column("oid", PgType.OID),
        column("tgrelid", PgType.OID),
        column("tgparentid", PgType.OID),
        column("tgname", PgType.NAME),
        column("tgfoid", PgType.OID),
        column("tgtype", PgType.INT2),
        column("tgenabled", PgType.CHAR),
        column("tgisinternal", PgType.BOOL),
        column("tgconstrrelid", PgType.OID),
        column("tgconstrindid", PgType.OID),
        column("tgconstraint", PgType.OID),
        column("tgdeferrable", PgType.BOOL),
        column("tginitdeferred", PgType.BOOL),
        column("tgnargs", PgType.INT2),
        column("tgattr", PgType.INT2VECTOR),
        column("tgargs", PgType.BYTEA),
        column("tgqual", PgType.NODE_TREE),
        column("tgoldtable", PgType.NAME),
        column("tgnewtable", PgType.NAME)
    ),
    pg_type(
        column("oid", PgType.OID),
        column("typname", PgType.NAME),
        column("typnamespace", PgType.OID),
        column("typowner", PgType.OID),
        column("typlen", PgType.INT2),
        column("typbyval", PgType.BOOL),
        column("typtype", PgType.CHAR),
        column("typisdefined", PgType.BOOL),
        column("typdelim", PgType.CHAR),
        column("typrelid", PgType.OID),
        column("typelem", PgType.OID),
        column("typinput", PgType.REGPROC),
        column("typoutput", PgType.REGPROC),
        column("typreceive", PgType.REGPROC),
        column("typsend", PgType.REGPROC),
        column("typanalyze", PgType.REGPROC),
        column("typalign", PgType.CHAR),
        column("typstorage", PgType.CHAR),
        column("typnotnull", PgType.BOOL),
        column("typbasetype", PgType.OID),
        column("typtypmod", PgType.INT4),
        column("typndims", PgType.INT4),
        column("typdefaultbin", PgType.NODE_TREE),
        column("typdefault", PgType.TEXT)
    );
    private final SchemaProperty[] properties;

    PgTable(Column... columns) {
        ArrayList<SchemaProperty> properties = new ArrayList<>();
        for (Column column : columns) {
            SqlTypeName sqlTypeName = PgTypeUtils.sqlTypeName(column.type);
            RelProtoDataType protoType = PgTypeUtils.protoType(column.type);
            properties.add(new SchemaProperty(column.name, sqlTypeName, protoType));
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

    private static Column column(String name, PgType type) {
        return new Column(name, type);
    }

    private static class Column {
        private final String name;
        private final PgType type;

        private Column(String name, PgType type) {
            this.name = name;
            this.type = type;
        }
    }
}
