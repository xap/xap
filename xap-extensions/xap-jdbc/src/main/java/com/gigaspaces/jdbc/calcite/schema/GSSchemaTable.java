package com.gigaspaces.jdbc.calcite.schema;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

public class GSSchemaTable extends AbstractTable {
    private final String name;
    private final SchemaProperty[] schemas;

    public GSSchemaTable(String name, SchemaProperty... schemas) {
        this.name = name;
        this.schemas = schemas;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
        for (SchemaProperty schema : this.schemas) {
            builder.add(schema.getPropertyName(), schema.getSqlTypeName());
        }
        return builder.build();
    }

    public String getName() {
        return name;
    }

    public SchemaProperty[] getSchemas() {
        return schemas;
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
