package com.gigaspaces.jdbc.model.table;

public class QueryColumn {
    private final String name;
    private final String alias;
    private final boolean isVisible;
    private final boolean isUUID;
    public static final String UUID_COLUMN = "UID";
    private final TableContainer tableContainer;

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

    public Object getCurrentValue(){
        if(tableContainer.getQueryResult().getCurrent() == null)
            return null;
        return tableContainer.getQueryResult().getCurrent().getPropertyValue(this);
    }
}
