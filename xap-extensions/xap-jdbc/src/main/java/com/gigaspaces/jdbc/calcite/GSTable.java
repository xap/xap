package com.gigaspaces.jdbc.calcite;

import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.metadata.PropertyInfo;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

public class GSTable extends AbstractTable {

    private final ITypeDesc typeDesc;

    public GSTable(ITypeDesc typeDesc) {
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

    public ITypeDesc getTypeDesc() {
        return typeDesc;
    }
}
