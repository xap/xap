package com.gigaspaces.jdbc.model.table;

public class OrderColumn extends QueryColumn {

    private boolean isAsc = true;
    private boolean isNullsLast = false;

    public OrderColumn(String name, boolean isVisible, TableContainer tableContainer) {
        super(name, null, null, isVisible, tableContainer);
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

    @Override
    public Object getCurrentValue() {
        if(tableContainer.getQueryResult().getCurrent() == null) {
            return null;
        }
        return tableContainer.getQueryResult().getCurrent().getPropertyValue(this); // visit getPropertyValue(OrderColumn)
    }

    @Override
    public String toString() {
        return getName() + " " + (isAsc ? "ASC" : "DESC") + " " + (isNullsLast ? "NULLS LAST" : "NULLS FIRST");
    }
}
