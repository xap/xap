package com.gigaspaces.jdbc.model.table;

import com.gigaspaces.internal.transport.IEntryPacket;

import java.util.Objects;

public class ConcreteColumn implements IQueryColumn {
    protected final TableContainer tableContainer;
    private final String columnName;
    private final String columnAlias;
    private final boolean isVisible;
    private final boolean isUUID;
    private final Class<?> returnType;
    private final int columnOrdinal;

    public ConcreteColumn(String columnName, Class<?> returnType, String columnAlias, boolean isVisible, TableContainer tableContainer, int columnOrdinal) {
        this.columnName = columnName;
        this.columnAlias = columnAlias == null ? columnName : columnAlias;
        this.isVisible = isVisible;
        this.isUUID = columnName.equalsIgnoreCase(UUID_COLUMN);
        this.tableContainer = tableContainer;
        this.returnType = returnType;
        this.columnOrdinal = columnOrdinal;
    }

    @Override
    public int getColumnOrdinal() {
        return columnOrdinal;
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
    public boolean isVisible() {
        return isVisible;
    }

    @Override
    public boolean isUUID() {
        return isUUID;
    }

    @Override
    public TableContainer getTableContainer() {
        return tableContainer;
    }

    @Override
    public Object getCurrentValue() {
        if (tableContainer.getQueryResult().getCurrent() == null)
            return null;
        return tableContainer.getQueryResult().getCurrent().getPropertyValue(this);
    }

    @Override
    public Class<?> getReturnType() {
        return returnType;
    }

    @Override
    public IQueryColumn create(String columnName, String columnAlias, boolean isVisible, int columnOrdinal) {
        return new ConcreteColumn(columnName, getReturnType(), columnAlias, isVisible, getTableContainer(), columnOrdinal);
    }

    @Override
    public String toString() {
        return tableContainer.getTableNameOrAlias() + "." + getAlias();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ConcreteColumn)) return false;
        IQueryColumn that = (IQueryColumn) o;
        return isVisible() == that.isVisible()
                && isUUID() == that.isUUID()
                && Objects.equals(getTableContainer(), that.getTableContainer())
                && Objects.equals(getName(), that.getName())
                && Objects.equals(getAlias(), that.getAlias());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getTableContainer(), getName(), getAlias(), isVisible(), isUUID());
    }

    @Override
    public int compareTo(IQueryColumn other) {
        return Integer.compare(this.getColumnOrdinal(), other.getColumnOrdinal());
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
}
