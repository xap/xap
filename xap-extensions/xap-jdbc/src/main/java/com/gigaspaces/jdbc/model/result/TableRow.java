package com.gigaspaces.jdbc.model.result;

import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.jdbc.model.table.ExplainPlanQueryColumn;
import com.gigaspaces.jdbc.model.table.QueryColumn;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class TableRow {
    private QueryColumn[] columns;
    private Object[] values;

    public TableRow(QueryColumn[] columns, Object[] values) {
        this.columns = columns;
        this.values = values;
    }

    public TableRow(IEntryPacket x, List<QueryColumn> queryColumns) {
        this.columns = queryColumns.toArray(new QueryColumn[0]);
        values = new Object[columns.length];
        for (int i = 0; i < queryColumns.size(); i++) {
            QueryColumn queryColumn = queryColumns.get(i);
            if (queryColumn.isUUID()) {
                values[i] = x.getUID();
            } else if (x.getTypeDescriptor().getIdPropertyName().equalsIgnoreCase(queryColumn.getName())) {
                values[i] = x.getID();
            } else {
                values[i] = x.getPropertyValue(queryColumn.getName());
            }
        }
    }

    public TableRow(List<QueryColumn> queryColumns) {
        this.columns = queryColumns.toArray(new QueryColumn[0]);
        values = new Object[columns.length];
        for (int i = 0; i < queryColumns.size(); i++) {
            Object value = queryColumns.get(i).getCurrentValue();
            values[i] = value;
        }
    }

    public TableRow(TableRow row, List<QueryColumn> queryColumns) {
        this.columns = queryColumns.toArray(new QueryColumn[0]);
        values = new Object[columns.length];
        for (int i = 0; i < queryColumns.size(); i++) {
            QueryColumn queryColumn = queryColumns.get(i);
            values[i] = row.getPropertyValue(queryColumn);
        }
    }

    public Object getPropertyValue(int index) {
        return values[index];
    }

    public Object getPropertyValue(QueryColumn column) {
        for (int i = 0; i < columns.length; i++) {
            if (columns[i].equals(column)) {
                return values[i];
            }
        }
        return null;
    }
}
