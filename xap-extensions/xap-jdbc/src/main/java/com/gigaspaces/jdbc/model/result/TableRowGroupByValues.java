package com.gigaspaces.jdbc.model.result;

import java.util.Arrays;

public class TableRowGroupByValues {

    private final Object[] groupByValues;

    public TableRowGroupByValues( Object[] groupByValues ){
        this.groupByValues = groupByValues;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TableRowGroupByValues that = (TableRowGroupByValues) o;
        return Arrays.equals(groupByValues, that.groupByValues);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(groupByValues);
    }
}