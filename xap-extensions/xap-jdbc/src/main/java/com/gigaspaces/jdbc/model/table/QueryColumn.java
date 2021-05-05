package com.gigaspaces.jdbc.model.table;

public class QueryColumn {
    private final String name;
    private final String alias;
    private final boolean isVisible;
    private final boolean isUUID;
    public static final String UUID_COLUMN = "UID";
    private final TableContainer tableContainer;
    private final Class<?> propertyType;

    public QueryColumn(String name, Class<?> propertyType, String alias, boolean isVisible, TableContainer tableContainer) {
        this.name = name;
        this.alias = alias;
        this.isVisible = isVisible;
        this.isUUID = name.equalsIgnoreCase(UUID_COLUMN);
        this.tableContainer = tableContainer;
        this.propertyType = propertyType;
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

    public Object getCurrentValue(){
        if(tableContainer.getQueryResult().getCurrent() == null)
            return null;
        return tableContainer.getQueryResult().getCurrent().getPropertyValue(this);
    }

    public Class<?> getPropertyType() {
        return propertyType;
    }

    @Override
    public String toString() {
        return tableContainer.getTableNameOrAlias() + "." + getNameOrAlias() ;
    }

    private String getNameOrAlias() {
        if(alias != null)
            return alias;
        return name;
    }
}
