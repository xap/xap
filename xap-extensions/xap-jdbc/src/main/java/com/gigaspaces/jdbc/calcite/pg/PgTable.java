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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;

import static com.gigaspaces.jdbc.calcite.pg.PgCalciteTable.SchemaProperty;

public enum PgTable {
    pg_am(
        column("oid", PgTypeDescriptor.OID),
        column("amname", PgTypeDescriptor.NAME),
        column("amhandler", PgTypeDescriptor.REGPROC),
        column("amtype", PgTypeDescriptor.CHAR)
    ),
    pg_attribute(
        column("attrelid", PgTypeDescriptor.OID),
        column("attname", PgTypeDescriptor.NAME),
        column("atttypid", PgTypeDescriptor.OID),
        column("attstattarget", PgTypeDescriptor.INT4),
        column("attlen", PgTypeDescriptor.INT2),
        column("attnum", PgTypeDescriptor.INT2),
        column("attndims", PgTypeDescriptor.INT4),
        column("attcacheoff", PgTypeDescriptor.INT4),
        column("atttypmod", PgTypeDescriptor.INT4),
        column("attbyval", PgTypeDescriptor.BOOL),
        column("attstorage", PgTypeDescriptor.CHAR),
        column("attalign", PgTypeDescriptor.CHAR),
        column("attnotnull", PgTypeDescriptor.BOOL),
        column("atthasdef", PgTypeDescriptor.BOOL),
        column("attidentity", PgTypeDescriptor.CHAR),
        column("attisdropped", PgTypeDescriptor.BOOL),
        column("attislocal", PgTypeDescriptor.BOOL),
        column("attinhcount", PgTypeDescriptor.INT4)
    ),
    pg_attrdef(
        column("adrelid", PgTypeDescriptor.OID),
        column("adnum", PgTypeDescriptor.INT2),
        column("adbin", PgTypeDescriptor.TEXT),
        column("adsrc", PgTypeDescriptor.TEXT)
    ),
    pg_class(
        column("oid", PgTypeDescriptor.OID),
        column("relname", PgTypeDescriptor.NAME),
        column("relnamespace", PgTypeDescriptor.OID),
        column("reltype", PgTypeDescriptor.OID),
        column("relowner", PgTypeDescriptor.OID),
        column("relam", PgTypeDescriptor.OID),
        column("relfilenode", PgTypeDescriptor.OID),
        column("reltablespace", PgTypeDescriptor.OID),
        column("relpages", PgTypeDescriptor.INT4),
        column("reltuples", PgTypeDescriptor.FLOAT4),
        column("reltoastrelid", PgTypeDescriptor.OID),
        column("relhasindex", PgTypeDescriptor.BOOL),
        column("relisshared", PgTypeDescriptor.BOOL),
        column("relkind", PgTypeDescriptor.CHAR),
        column("relnatts", PgTypeDescriptor.INT2),
        column("relchecks", PgTypeDescriptor.INT2),
        column("reltriggers", PgTypeDescriptor.INT2),
        column("relhasrules", PgTypeDescriptor.BOOL),
        column("relhastriggers", PgTypeDescriptor.BOOL),
        column("relhassubclass", PgTypeDescriptor.BOOL),
        column("relacl", PgTypeDescriptor.UNKNOWN),
        column("reloptions", PgTypeDescriptor.TEXT.asArray())
    ),
    pg_constraint(
        column("oid", PgTypeDescriptor.OID),
        column("conname", PgTypeDescriptor.NAME),
        column("connamespace", PgTypeDescriptor.OID),
        column("contype", PgTypeDescriptor.CHAR),
        column("condeferrable", PgTypeDescriptor.BOOL),
        column("condeferred", PgTypeDescriptor.BOOL),
        column("convalidated", PgTypeDescriptor.BOOL),
        column("conrelid", PgTypeDescriptor.OID),
        column("contypid", PgTypeDescriptor.OID),
        column("conindid", PgTypeDescriptor.OID),
        column("conparentid", PgTypeDescriptor.OID),
        column("confrelid", PgTypeDescriptor.OID),
        column("confupdtype", PgTypeDescriptor.OID),
        column("confdeltype", PgTypeDescriptor.OID),
        column("confmatchtype", PgTypeDescriptor.CHAR),
        column("conislocal", PgTypeDescriptor.CHAR),
        column("coninhcount", PgTypeDescriptor.CHAR),
        column("connoinherit", PgTypeDescriptor.BOOL),
        column("conkey", PgTypeDescriptor.INT2.asArray()),
        column("confkey", PgTypeDescriptor.INT2.asArray()),
        column("conpfeqop", PgTypeDescriptor.OID.asArray()),
        column("conppeqop", PgTypeDescriptor.OID.asArray()),
        column("conffeqop", PgTypeDescriptor.OID.asArray()),
        column("conexclop", PgTypeDescriptor.OID.asArray()),
        column("conbin", PgTypeDescriptor.NODE_TREE)
    ),
    pg_database(
        column("oid", PgTypeDescriptor.OID),
        column("datname", PgTypeDescriptor.NAME),
        column("datdba", PgTypeDescriptor.OID),
        column("encoding", PgTypeDescriptor.INT4),
        column("datcollate", PgTypeDescriptor.NAME),
        column("datctype", PgTypeDescriptor.NAME),
        column("datistemplate", PgTypeDescriptor.BOOL),
        column("datallowconn", PgTypeDescriptor.BOOL),
        column("datconnlimit", PgTypeDescriptor.INT4),
        column("datlastsysoid", PgTypeDescriptor.OID),
        column("datfrozenxid", PgTypeDescriptor.UNKNOWN),
        column("datminmxid", PgTypeDescriptor.UNKNOWN),
        column("dattablespace", PgTypeDescriptor.OID),
        column("datacl", PgTypeDescriptor.UNKNOWN)
    ),
    pg_index(
        column("oid", PgTypeDescriptor.OID),
        column("indexrelid", PgTypeDescriptor.OID),
        column("indrelid", PgTypeDescriptor.OID),
        column("indnatts", PgTypeDescriptor.INT2),
        column("indnkeyatts", PgTypeDescriptor.INT2),
        column("indisunique", PgTypeDescriptor.BOOL),
        column("indisprimary", PgTypeDescriptor.BOOL),
        column("indisexclusion", PgTypeDescriptor.BOOL),
        column("indimmediate", PgTypeDescriptor.BOOL),
        column("indisclustered", PgTypeDescriptor.BOOL),
        column("indisvalid", PgTypeDescriptor.BOOL),
        column("indcheckxmin", PgTypeDescriptor.BOOL),
        column("indisready", PgTypeDescriptor.BOOL),
        column("indislive", PgTypeDescriptor.BOOL),
        column("indisreplident", PgTypeDescriptor.BOOL),
        column("indkey", PgTypeDescriptor.INT2VECTOR),
        column("indcollation", PgTypeDescriptor.OID_VECTOR),
        column("indclass", PgTypeDescriptor.OID_VECTOR),
        column("indoption", PgTypeDescriptor.INT2VECTOR),
        column("indexprs", PgTypeDescriptor.NODE_TREE),
        column("indpred", PgTypeDescriptor.NODE_TREE)
    ),
    pg_namespace(
        column("oid", PgTypeDescriptor.OID),
        column("nspname", PgTypeDescriptor.NAME),
        column("nspowner", PgTypeDescriptor.OID),
        column("nspacl", PgTypeDescriptor.UNKNOWN)
    ),
    pg_proc(
        column("oid", PgTypeDescriptor.OID),
        column("proname", PgTypeDescriptor.NAME),
        column("pronamespace", PgTypeDescriptor.OID),
        column("proowner", PgTypeDescriptor.OID),
        column("prolang", PgTypeDescriptor.OID),
        column("procost", PgTypeDescriptor.FLOAT4),
        column("prorows", PgTypeDescriptor.FLOAT4),
        column("provariadic", PgTypeDescriptor.OID),
        column("prosupport", PgTypeDescriptor.REGPROC),
        column("prokind", PgTypeDescriptor.CHAR),
        column("prosecdef", PgTypeDescriptor.BOOL),
        column("proleakproof", PgTypeDescriptor.BOOL),
        column("proisstrict", PgTypeDescriptor.BOOL),
        column("proretset", PgTypeDescriptor.BOOL),
        column("provolatile", PgTypeDescriptor.CHAR),
        column("proparallel", PgTypeDescriptor.CHAR),
        column("pronargs", PgTypeDescriptor.INT2),
        column("pronargdefaults", PgTypeDescriptor.INT2),
        column("prorettype", PgTypeDescriptor.OID),
        column("proargtypes", PgTypeDescriptor.OID_VECTOR),
        column("proallargtypes", PgTypeDescriptor.OID.asArray()),
        column("proargmodes", PgTypeDescriptor.CHAR.asArray()),
        column("proargnames", PgTypeDescriptor.TEXT.asArray()),
        column("proargdefaults", PgTypeDescriptor.NODE_TREE),
        column("protrftypes", PgTypeDescriptor.OID.asArray()),
        column("prosrc", PgTypeDescriptor.TEXT),
        column("probin", PgTypeDescriptor.TEXT),
        column("proconfig", PgTypeDescriptor.TEXT.asArray()),
        column("proacl", PgTypeDescriptor.UNKNOWN)
    ),
    pg_trigger(
        column("oid", PgTypeDescriptor.OID),
        column("tgrelid", PgTypeDescriptor.OID),
        column("tgparentid", PgTypeDescriptor.OID),
        column("tgname", PgTypeDescriptor.NAME),
        column("tgfoid", PgTypeDescriptor.OID),
        column("tgtype", PgTypeDescriptor.INT2),
        column("tgenabled", PgTypeDescriptor.CHAR),
        column("tgisinternal", PgTypeDescriptor.BOOL),
        column("tgconstrrelid", PgTypeDescriptor.OID),
        column("tgconstrindid", PgTypeDescriptor.OID),
        column("tgconstraint", PgTypeDescriptor.OID),
        column("tgdeferrable", PgTypeDescriptor.BOOL),
        column("tginitdeferred", PgTypeDescriptor.BOOL),
        column("tgnargs", PgTypeDescriptor.INT2),
        column("tgattr", PgTypeDescriptor.INT2VECTOR),
        column("tgargs", PgTypeDescriptor.BYTEA),
        column("tgqual", PgTypeDescriptor.NODE_TREE),
        column("tgoldtable", PgTypeDescriptor.NAME),
        column("tgnewtable", PgTypeDescriptor.NAME)
    ),
    pg_type(
        column("oid", PgTypeDescriptor.OID),
        column("typname", PgTypeDescriptor.NAME),
        column("typnamespace", PgTypeDescriptor.OID),
        column("typowner", PgTypeDescriptor.OID),
        column("typlen", PgTypeDescriptor.INT2),
        column("typbyval", PgTypeDescriptor.BOOL),
        column("typtype", PgTypeDescriptor.CHAR),
        column("typisdefined", PgTypeDescriptor.BOOL),
        column("typdelim", PgTypeDescriptor.CHAR),
        column("typrelid", PgTypeDescriptor.OID),
        column("typelem", PgTypeDescriptor.OID),
        column("typinput", PgTypeDescriptor.REGPROC),
        column("typoutput", PgTypeDescriptor.REGPROC),
        column("typreceive", PgTypeDescriptor.REGPROC),
        column("typsend", PgTypeDescriptor.REGPROC),
        column("typanalyze", PgTypeDescriptor.REGPROC),
        column("typalign", PgTypeDescriptor.CHAR),
        column("typstorage", PgTypeDescriptor.CHAR),
        column("typnotnull", PgTypeDescriptor.BOOL),
        column("typbasetype", PgTypeDescriptor.OID),
        column("typtypmod", PgTypeDescriptor.INT4),
        column("typndims", PgTypeDescriptor.INT4),
        column("typdefaultbin", PgTypeDescriptor.NODE_TREE),
        column("typdefault", PgTypeDescriptor.TEXT)
    );
    private final SchemaProperty[] properties;

    PgTable(Column... columns) {
        ArrayList<SchemaProperty> properties = new ArrayList<>();
        for (Column column : columns) {
            SqlTypeName sqlTypeName = PgTypeUtils.toSqlTypeName(column.type);
            RelProtoDataType protoType = PgTypeUtils.toRelProtoDataType(column.type);
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

    private static Column column(String name, PgTypeDescriptor type) {
        return new Column(name, type);
    }

    private static class Column {
        private final String name;
        private final PgTypeDescriptor type;

        private Column(String name, PgTypeDescriptor type) {
            this.name = name;
            this.type = type;
        }
    }
}
