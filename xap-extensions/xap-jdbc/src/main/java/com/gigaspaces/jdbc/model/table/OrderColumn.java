package com.gigaspaces.jdbc.model.table;

public class OrderColumn extends QueryColumn {

    private boolean isDesc = false;

    public OrderColumn(String name, String alias, boolean isVisible, TableContainer tableContainer) {
        super(name, alias, isVisible, tableContainer);
    }

    public boolean isDesc() {
        return isDesc;
    }

    public void setDesc(boolean isDesc) {
        this.isDesc = isDesc;
    }
}
