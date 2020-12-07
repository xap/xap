package com.j_spaces.jdbc;

import com.gigaspaces.internal.transport.IEntryPacket;

import java.sql.SQLException;

public class ValueSelectColumn extends SelectColumn {
    private final Object value;

    public ValueSelectColumn(Object value) {
        this.value = value;
    }

    @Override
    public void createColumnData(AbstractDMLQuery query) throws SQLException {
        super.createColumnData(query);
    }

    @Override
    public boolean isAllColumns() {
        return false;
    }

    @Override
    public boolean isUid() {
        return false;
    }

    @Override
    public Object getFieldValue(IEntryPacket entry) {
        return value;
    }

    @Override
    public Object getValue() {
        return value;
    }

    @Override
    public void setAlias(String alias) {
        super.setAlias(alias);
        if (this.getName() == null) this.setName(alias);
    }
}
