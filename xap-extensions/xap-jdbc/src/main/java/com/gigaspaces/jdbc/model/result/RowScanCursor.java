package com.gigaspaces.jdbc.model.result;

import java.util.Iterator;
import java.util.List;

public class RowScanCursor implements Cursor<TableRow> {
    private final List<TableRow> rows;
    private Iterator<TableRow> iterator;
    private TableRow current;

    public RowScanCursor(List<TableRow> rows) {
        this.rows = rows;
    }

    @Override
    public boolean next() {
        if (iterator().hasNext()) {
            current = iterator.next();
            return true;
        }
        return false;
    }

    @Override
    public TableRow getCurrent() {
        return current;
    }

    @Override
    public void reset() {
        iterator = null;
    }

    @Override
    public boolean isBeforeFirst() {
        return iterator == null;
    }

    private Iterator<TableRow> iterator() {
        if (iterator == null)
            iterator = rows.iterator();
        return iterator;
    }
}
