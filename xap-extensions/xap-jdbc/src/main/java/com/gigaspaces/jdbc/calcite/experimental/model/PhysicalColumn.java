package com.gigaspaces.jdbc.calcite.experimental.model;

import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.jdbc.calcite.experimental.ResultSupplier;

import java.sql.SQLException;
import java.util.Objects;

public class PhysicalColumn implements IQueryColumn {
    protected final ResultSupplier resultSupplier;
    private final String columnName;
    private final String columnAlias;
    private final boolean isUUID;
    private Class<?> returnType;

    public PhysicalColumn(String columnName, String columnAlias, ResultSupplier resultSupplier) {
        this.columnName = columnName;
        this.columnAlias = columnAlias == null ? columnName : columnAlias;
        this.isUUID = columnName.equalsIgnoreCase(UUID_COLUMN);
        this.resultSupplier = resultSupplier;
        try {
            this.returnType = resultSupplier.getReturnType(columnName);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    @Override
    public String getName() {
        return columnName;
    }

    @Override
    public String getAlias() {
        return columnAlias;
    }

    @Override
    public boolean isUUID() {
        return isUUID;
    }

    @Override
    public ResultSupplier getResultSupplier() {
        return resultSupplier;
    }

    @Override
    public Object getCurrentValue() {
        if (resultSupplier.getQueryResult().getCurrent() == null)
            return null;
        return resultSupplier.getQueryResult().getCurrent().getPropertyValue(this);
    }

    @Override
    public Class<?> getReturnType() {
        return returnType;
    }

//    @Override
//    public IQueryColumn create(String columnName, String columnAlias) {
//        return new ConcreteColumn(columnName, columnAlias, getResultSupplier(), columnOrdinal);
//    }

    @Override
    public String toString() {
        return resultSupplier.getTableNameOrAlias() + "." + getAlias();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PhysicalColumn)) return false;
        IQueryColumn that = (IQueryColumn) o;
        return isUUID() == that.isUUID()
                && Objects.equals(getResultSupplier(), that.getResultSupplier())
                && Objects.equals(getName(), that.getName())
                && Objects.equals(getAlias(), that.getAlias());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getResultSupplier(), getName(), getAlias(), isUUID());
    }

    @Override
    public Object getValue(IEntryPacket entryPacket) {
        Object value;
        if (isUUID()) {
            value = entryPacket.getUID();
        } else if (entryPacket.getTypeDescriptor().getIdPropertyName().equalsIgnoreCase(getName())) {
            value = entryPacket.getID();
        } else {
            value = entryPacket.getPropertyValue(getName());
        }
        return value;
    }

    @Override
    public boolean isPhysical() {
        return true;
    }
}
