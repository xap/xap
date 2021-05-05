package com.gigaspaces.jdbc.model.table;

public class OrderColumn extends QueryColumn {

    private boolean isAsc = true;

    public OrderColumn(String name, String alias, boolean isVisible, TableContainer tableContainer) {
        super(name, alias, isVisible, tableContainer);
    }

    public boolean isAsc() {
        return isAsc;
    }

    public void setAsc(boolean isAsc) {
        this.isAsc = isAsc;
    }
}
