package com.gigaspaces.jdbc.calcite.schema;

import com.gigaspaces.jdbc.calcite.schema.GSSchemaTable.SchemaProperty;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.List;

public enum PGSystemTable {
    pg_am(new PropertiesBuilder().property("oid", SqlTypeName.INTEGER).property("amname", SqlTypeName.VARCHAR)),
    pg_attrdef(new PropertiesBuilder().property("oid", SqlTypeName.INTEGER).property("adrelid", SqlTypeName.INTEGER).property("adnum", SqlTypeName.INTEGER).property("adbin", SqlTypeName.VARCHAR)),
//    pg_attribute,
//    pg_authid,
//    pg_class,
//    pg_constraint,
//    pg_database,
//    pg_description,
//    pg_group,
//    pg_index,
//    pg_inherits,
//    pg_namespace,
//    pg_proc,
//    pg_roles,
    pg_tables(new PropertiesBuilder().property("tablename", SqlTypeName.VARCHAR).property("hasindexes", SqlTypeName.BOOLEAN)),
//    pg_settings,
//    pg_tablespace,
//    pg_trigger,
//    pg_type,
//    pg_user
    ;
    private final SchemaProperty[] properties;

    PGSystemTable(PropertiesBuilder builder) {
        this.properties = builder.properties.toArray(new SchemaProperty[0]);
    }

    PGSystemTable() {
        this.properties = new SchemaProperty[0];
    }

    public SchemaProperty[] getProperties() {
        return properties;
    }

    public static class PropertiesBuilder {
        private final List<SchemaProperty> properties = new ArrayList<>();
        public PropertiesBuilder property(String name, SqlTypeName typeName) {
            properties.add(SchemaProperty.of(name, typeName));
            return this;
        }
    }
}
