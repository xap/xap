package com.gigaspaces.jdbc.calcite;

import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.metadata.PropertyInfo;
import com.gigaspaces.jdbc.model.table.ConcreteTableContainer;
import com.gigaspaces.jdbc.model.table.TableContainer;
import com.j_spaces.core.IJSpace;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

public class GSTableImpl extends AbstractTable implements GSTable {

    private final ITypeDesc typeDesc;
    private final String name;

    public GSTableImpl(String name, ITypeDesc typeDesc) {
        this.name = name;
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

    @Override
    public String getName() {
        return name;
    }

    @Override
    public TableContainer createTableContainer(IJSpace space) {
        return new ConcreteTableContainer(getName(), null, space);
    }
}
