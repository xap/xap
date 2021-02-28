package com.gigaspaces.jdbc.model.table;

public class QueryColumn implements Comparable {
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

    @Override
    public int compareTo(Object o) {
        if (o instanceof QueryColumn) {
            return name.compareTo(((QueryColumn) o).getName());
        }
        return 0;
    }
}
