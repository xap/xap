package com.gigaspaces.jdbc.model.table;

public class OrderColumn extends QueryColumn {

    private boolean isAsc = true;
    private boolean isNullsLast = false;

    public OrderColumn(String name, String alias, boolean isVisible, TableContainer tableContainer) {
        super(name, alias, isVisible, tableContainer);
    }

    public OrderColumn(String name, boolean isVisible, TableContainer tableContainer) {
        super(name, null, isVisible, tableContainer);
    }

    public boolean isAsc() {
        return isAsc;
    }
    public void setAsc(boolean isAsc) {
        this.isAsc = isAsc;
    }
    public OrderColumn withAsc(boolean isAsc) {
        this.isAsc = isAsc;
        return this;
    }

    public boolean isNullsLast() {
        return isNullsLast;
    }
    public void setNullsLast(boolean isNullsLast) {
        this.isNullsLast = isNullsLast;
    }
    public OrderColumn withNullsLast(boolean isNullsLast) {
        this.isNullsLast = isNullsLast;
        return this;
    }
}
