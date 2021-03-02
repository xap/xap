package com.gigaspaces.jdbc.model.table;

public class QueryColumn {
    private final String name;
    private final String alias;
    private final boolean isVisible;

    public QueryColumn(String name, String alias, boolean isVisible) {
        this.name = name;
        this.alias = alias;
        this.isVisible = isVisible;
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
}
