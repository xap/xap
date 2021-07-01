package com.gigaspaces.jdbc.calcite.schema;

import com.gigaspaces.jdbc.calcite.schema.type.PgType;
import com.gigaspaces.jdbc.calcite.schema.type.TypeBool;
import com.gigaspaces.jdbc.calcite.schema.type.TypeBytea;
import com.gigaspaces.jdbc.calcite.schema.type.TypeChar;
import com.gigaspaces.jdbc.calcite.schema.type.TypeFloat4;
import com.gigaspaces.jdbc.calcite.schema.type.TypeInt2;
import com.gigaspaces.jdbc.calcite.schema.type.TypeInt2Vector;
import com.gigaspaces.jdbc.calcite.schema.type.TypeInt4;
import com.gigaspaces.jdbc.calcite.schema.type.TypeName;
import com.gigaspaces.jdbc.calcite.schema.type.TypeNodeTree;
import com.gigaspaces.jdbc.calcite.schema.type.TypeOid;
import com.gigaspaces.jdbc.calcite.schema.type.TypeOidVector;
import com.gigaspaces.jdbc.calcite.schema.type.TypeRegproc;
import com.gigaspaces.jdbc.calcite.schema.type.TypeText;
import com.gigaspaces.jdbc.calcite.schema.type.TypeUnknown;
import com.gigaspaces.jdbc.calcite.schema.type.TypeUtils;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;

import static com.gigaspaces.jdbc.calcite.schema.GSSchemaTable.SchemaProperty;

public enum PGSystemTable {
    pg_am(
        column("oid", TypeOid.INSTANCE),
        column("amname", TypeName.INSTANCE),
        column("amhandler", TypeRegproc.INSTANCE),
        column("amtype", TypeChar.INSTANCE)
    ),
    pg_attribute(
        column("attrelid", TypeOid.INSTANCE),
        column("attname", TypeName.INSTANCE),
        column("atttypid", TypeOid.INSTANCE),
        column("attstattarget", TypeInt4.INSTANCE),
        column("attlen", TypeInt2.INSTANCE),
        column("attnum", TypeInt2.INSTANCE),
        column("attndims", TypeInt4.INSTANCE),
        column("attcacheoff", TypeInt4.INSTANCE),
        column("atttypmod", TypeInt4.INSTANCE),
        column("attbyval", TypeBool.INSTANCE),
        column("attstorage", TypeChar.INSTANCE),
        column("attalign", TypeChar.INSTANCE),
        column("attnotnull", TypeBool.INSTANCE),
        column("atthasdef", TypeBool.INSTANCE),
        column("attisdropped", TypeBool.INSTANCE),
        column("attislocal", TypeBool.INSTANCE),
        column("attinhcount", TypeInt4.INSTANCE)
    ),
    pg_class(
        column("oid", TypeOid.INSTANCE),
        column("relname", TypeName.INSTANCE),
        column("relnamespace", TypeOid.INSTANCE),
        column("reltype", TypeOid.INSTANCE),
        column("relowner", TypeOid.INSTANCE),
        column("relam", TypeOid.INSTANCE),
        column("relfilenode", TypeOid.INSTANCE),
        column("reltablespace", TypeOid.INSTANCE),
        column("relpages", TypeInt4.INSTANCE),
        column("reltuples", TypeFloat4.INSTANCE),
        column("reltoastrelid", TypeOid.INSTANCE),
        column("relhasindex", TypeBool.INSTANCE),
        column("relisshared", TypeBool.INSTANCE),
        column("relkind", TypeChar.INSTANCE),
        column("relnatts", TypeInt2.INSTANCE),
        column("relchecks", TypeInt2.INSTANCE),
        column("reltriggers", TypeInt2.INSTANCE),
        column("relhasrules", TypeBool.INSTANCE),
        column("relhastriggers", TypeBool.INSTANCE),
        column("relhassubclass", TypeBool.INSTANCE),
        column("relacl", TypeUnknown.INSTANCE),
        column("reloptions", TypeText.INSTANCE.asArray())
    ),
    pg_constraint(
        column("oid", TypeOid.INSTANCE),
        column("conname", TypeName.INSTANCE),
        column("connamespace", TypeOid.INSTANCE),
        column("contype", TypeChar.INSTANCE),
        column("condeferrable", TypeBool.INSTANCE),
        column("condeferred", TypeBool.INSTANCE),
        column("convalidated", TypeBool.INSTANCE),
        column("conrelid", TypeOid.INSTANCE),
        column("contypid", TypeOid.INSTANCE),
        column("conindid", TypeOid.INSTANCE),
        column("conparentid", TypeOid.INSTANCE),
        column("confrelid", TypeOid.INSTANCE),
        column("confupdtype", TypeOid.INSTANCE),
        column("confdeltype", TypeOid.INSTANCE),
        column("confmatchtype", TypeChar.INSTANCE),
        column("conislocal", TypeChar.INSTANCE),
        column("coninhcount", TypeChar.INSTANCE),
        column("connoinherit", TypeBool.INSTANCE),
        column("conkey", TypeInt2.INSTANCE.asArray()),
        column("confkey", TypeInt2.INSTANCE.asArray()),
        column("conpfeqop", TypeOid.INSTANCE.asArray()),
        column("conppeqop", TypeOid.INSTANCE.asArray()),
        column("conffeqop", TypeOid.INSTANCE.asArray()),
        column("conexclop", TypeOid.INSTANCE.asArray()),
        column("conbin", TypeNodeTree.INSTANCE)
    ),
    pg_database(
        column("oid", TypeOid.INSTANCE),
        column("datname", TypeName.INSTANCE),
        column("datdba", TypeOid.INSTANCE),
        column("encoding", TypeInt4.INSTANCE),
        column("datcollate", TypeName.INSTANCE),
        column("datctype", TypeName.INSTANCE),
        column("datistemplate", TypeBool.INSTANCE),
        column("datallowconn", TypeBool.INSTANCE),
        column("datconnlimit", TypeInt4.INSTANCE),
        column("datlastsysoid", TypeOid.INSTANCE),
        column("datfrozenxid", TypeUnknown.INSTANCE),
        column("datminmxid", TypeUnknown.INSTANCE),
        column("dattablespace", TypeOid.INSTANCE),
        column("datacl", TypeUnknown.INSTANCE)
    ),
    pg_index(
        column("oid", TypeOid.INSTANCE),
        column("indexrelid", TypeOid.INSTANCE),
        column("indrelid", TypeOid.INSTANCE),
        column("indnatts", TypeInt2.INSTANCE),
        column("indnkeyatts", TypeInt2.INSTANCE),
        column("indisunique", TypeBool.INSTANCE),
        column("indisprimary", TypeBool.INSTANCE),
        column("indisexclusion", TypeBool.INSTANCE),
        column("indimmediate", TypeBool.INSTANCE),
        column("indisclustered", TypeBool.INSTANCE),
        column("indisvalid", TypeBool.INSTANCE),
        column("indcheckxmin", TypeBool.INSTANCE),
        column("indisready", TypeBool.INSTANCE),
        column("indislive", TypeBool.INSTANCE),
        column("indisreplident", TypeBool.INSTANCE),
        column("indkey", TypeInt2Vector.INSTANCE),
        column("indcollation", TypeOidVector.INSTANCE),
        column("indclass", TypeOidVector.INSTANCE),
        column("indoption", TypeInt2Vector.INSTANCE),
        column("indexprs", TypeNodeTree.INSTANCE),
        column("indpred", TypeNodeTree.INSTANCE)
    ),
    pg_namespace(
        column("oid", TypeOid.INSTANCE),
        column("nspname", TypeName.INSTANCE),
        column("nspowner", TypeOid.INSTANCE),
        column("nspacl", TypeUnknown.INSTANCE)
    ),
    pg_proc(
        column("oid", TypeOid.INSTANCE),
        column("proname", TypeName.INSTANCE),
        column("pronamespace", TypeOid.INSTANCE),
        column("proowner", TypeOid.INSTANCE),
        column("prolang", TypeOid.INSTANCE),
        column("procost", TypeFloat4.INSTANCE),
        column("prorows", TypeFloat4.INSTANCE),
        column("provariadic", TypeOid.INSTANCE),
        column("prosupport", TypeRegproc.INSTANCE),
        column("prokind", TypeChar.INSTANCE),
        column("prosecdef", TypeBool.INSTANCE),
        column("proleakproof", TypeBool.INSTANCE),
        column("proisstrict", TypeBool.INSTANCE),
        column("proretset", TypeBool.INSTANCE),
        column("provolatile", TypeChar.INSTANCE),
        column("proparallel", TypeChar.INSTANCE),
        column("pronargs", TypeInt2.INSTANCE),
        column("pronargdefaults", TypeInt2.INSTANCE),
        column("prorettype", TypeOid.INSTANCE),
        column("proargtypes", TypeOidVector.INSTANCE),
        column("proallargtypes", TypeOid.INSTANCE.asArray()),
        column("proargmodes", TypeChar.INSTANCE.asArray()),
        column("proargnames", TypeText.INSTANCE.asArray()),
        column("proargdefaults", TypeNodeTree.INSTANCE),
        column("protrftypes", TypeOid.INSTANCE.asArray()),
        column("prosrc", TypeText.INSTANCE),
        column("probin", TypeText.INSTANCE),
        column("proconfig", TypeText.INSTANCE.asArray()),
        column("proacl", TypeUnknown.INSTANCE)
    ),
    pg_trigger(
        column("oid", TypeOid.INSTANCE),
        column("tgrelid", TypeOid.INSTANCE),
        column("tgparentid", TypeOid.INSTANCE),
        column("tgname", TypeName.INSTANCE),
        column("tgfoid", TypeOid.INSTANCE),
        column("tgtype", TypeInt2.INSTANCE),
        column("tgenabled", TypeChar.INSTANCE),
        column("tgisinternal", TypeBool.INSTANCE),
        column("tgconstrrelid", TypeOid.INSTANCE),
        column("tgconstrindid", TypeOid.INSTANCE),
        column("tgconstraint", TypeOid.INSTANCE),
        column("tgdeferrable", TypeBool.INSTANCE),
        column("tginitdeferred", TypeBool.INSTANCE),
        column("tgnargs", TypeInt2.INSTANCE),
        column("tgattr", TypeInt2Vector.INSTANCE),
        column("tgargs", TypeBytea.INSTANCE),
        column("tgqual", TypeNodeTree.INSTANCE),
        column("tgoldtable", TypeName.INSTANCE),
        column("tgnewtable", TypeName.INSTANCE)
    ),
    pg_type(
        column("oid", TypeOid.INSTANCE),
        column("typname", TypeName.INSTANCE),
        column("typnamespace", TypeOid.INSTANCE),
        column("typowner", TypeOid.INSTANCE),
        column("typlen", TypeInt2.INSTANCE),
        column("typbyval", TypeBool.INSTANCE),
        column("typtype", TypeChar.INSTANCE),
        column("typisdefined", TypeBool.INSTANCE),
        column("typdelim", TypeChar.INSTANCE),
        column("typrelid", TypeOid.INSTANCE),
        column("typelem", TypeOid.INSTANCE),
        column("typinput", TypeRegproc.INSTANCE),
        column("typoutput", TypeRegproc.INSTANCE),
        column("typreceive", TypeRegproc.INSTANCE),
        column("typsend", TypeRegproc.INSTANCE),
        column("typanalyze", TypeRegproc.INSTANCE),
        column("typalign", TypeChar.INSTANCE),
        column("typstorage", TypeChar.INSTANCE),
        column("typnotnull", TypeBool.INSTANCE),
        column("typbasetype", TypeOid.INSTANCE),
        column("typtypmod", TypeInt4.INSTANCE),
        column("typndims", TypeInt4.INSTANCE),
        column("typdefaultbin", TypeNodeTree.INSTANCE),
        column("typdefault", TypeText.INSTANCE)
    );
    private final SchemaProperty[] properties;

    PGSystemTable(Column... columns) {
        ArrayList<SchemaProperty> properties = new ArrayList<>();
        for (Column column : columns) {
            SqlTypeName sqlTypeName = TypeUtils.sqlTypeName(column.type);
            RelProtoDataType protoType = TypeUtils.protoType(column.type);
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
