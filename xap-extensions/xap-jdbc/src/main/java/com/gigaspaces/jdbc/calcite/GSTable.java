package com.gigaspaces.jdbc.calcite;

import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.metadata.PropertyInfo;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.math.BigDecimal;

public class GSTable extends AbstractTable {

    private final ITypeDesc typeDesc;

    public GSTable(ITypeDesc typeDesc) {
        this.typeDesc = typeDesc;
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

    public ITypeDesc getTypeDesc() {
        return typeDesc;
    }
}
