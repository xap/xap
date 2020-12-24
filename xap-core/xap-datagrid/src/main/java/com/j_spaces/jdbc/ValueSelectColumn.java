package com.j_spaces.jdbc;

import com.gigaspaces.internal.transport.IEntryPacket;

import java.sql.SQLException;

public class ValueSelectColumn extends SelectColumn {
    private final Object value;
    private boolean isConsumed;

    public ValueSelectColumn(Object value) {
        this.value = value;
    }

    @Override
    public void createColumnData(AbstractDMLQuery query) throws SQLException {
        super.createColumnData(query);
    }

    @Override
    public Object getFieldValue(IEntryPacket entry) {
        if (isConsumed) return null;
        isConsumed = true;
        return value;
    }

    @Override
    public Object getValue() {
        if (isConsumed) return null;
        isConsumed = true;
        return value;
    }

}
