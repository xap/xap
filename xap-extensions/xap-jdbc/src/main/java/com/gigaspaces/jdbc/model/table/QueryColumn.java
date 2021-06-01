package com.gigaspaces.jdbc.model.table;

import java.util.Objects;

public class QueryColumn {
    public static final String UUID_COLUMN = "UID";
    protected final TableContainer tableContainer;
    private final String name;
    private final String alias;
    private final boolean isVisible;
    private final boolean isUUID;

    public QueryColumn(String name, String alias, boolean isVisible, TableContainer tableContainer) {
        this.name = name;
        this.alias = alias;
        this.isVisible = isVisible;
        this.isUUID = name.equalsIgnoreCase(UUID_COLUMN);
        this.tableContainer = tableContainer;
    }

    public String getName() {
        return name;
    }

    public String getAlias() {
        return alias;
    }

    public boolean isVisible() {
        return isVisible;
    }

    public boolean isUUID() {
        return isUUID;
    }

    public TableContainer getTableContainer() {
        return tableContainer;
    }

    public Object getCurrentValue() {
        if (tableContainer.getQueryResult().getCurrent() == null)
            return null;
        return tableContainer.getQueryResult().getCurrent().getPropertyValue(this);
    }

    @Override
    public String toString() {
        return tableContainer.getTableNameOrAlias() + "." + getNameOrAlias();
    }

    public String getNameOrAlias() {
        if (alias != null)
            return alias;
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof QueryColumn)) return false;
        QueryColumn that = (QueryColumn) o;
        return isVisible() == that.isVisible() && isUUID() == that.isUUID() && Objects.equals(getTableContainer(), that.getTableContainer()) && Objects.equals(getName(), that.getName()) && Objects.equals(getAlias(), that.getAlias());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getTableContainer(), getName(), getAlias(), isVisible(), isUUID());
    }
}
