package com.gigaspaces.jdbc;

import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.jdbc.model.table.QueryColumn;

import java.util.List;

public class TableEntry {
    private QueryColumn[] columns;
    private Object[] values;

    public TableEntry(IEntryPacket x, List<QueryColumn> queryColumns) {
        this.columns = queryColumns.toArray(new QueryColumn[0]);
        values = new Object[columns.length];
        for (int i = 0; i < queryColumns.size(); i++) {
            values[i] = x.getPropertyValue(queryColumns.get(i).getName());
        }
    }

    public Object getPropertyValue(int index) {
        return values[index];
    }
}
