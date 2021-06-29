package com.gigaspaces.jdbc.model.table;

import com.gigaspaces.internal.transport.IEntryPacket;

public class LiteralColumn implements IQueryColumn{
    private final Object value;

    public LiteralColumn(Object value) {
        this.value = value;
    }

    @Override
    public int getColumnOrdinal() {
        throw new UnsupportedOperationException("Unsupported method getColumnOrdinal");
    }

    @Override
    public String getName() {
        throw new UnsupportedOperationException("Unsupported method getName");
    }

    @Override
    public String getAlias() {
        throw new UnsupportedOperationException("Unsupported method getName");
    }

    @Override
    public boolean isVisible() {
        throw new UnsupportedOperationException("Unsupported method getName");
    }

    @Override
    public boolean isUUID() {
        throw new UnsupportedOperationException("Unsupported method getName");
    }

    @Override
    public TableContainer getTableContainer() {
        throw new UnsupportedOperationException("Unsupported method getName");
    }

    @Override
    public Object getCurrentValue() {
        return value;
    }

    @Override
    public Class<?> getReturnType() {
        return value.getClass();
    }

    @Override
    public IQueryColumn create(String columnName, String columnAlias, boolean isVisible, int columnOrdinal) {
        throw new UnsupportedOperationException("Unsupported method getName");
    }

    @Override
    public int compareTo(IQueryColumn o) {
        throw new UnsupportedOperationException("Unsupported method getName");
    }

    @Override
    public Object getValue(IEntryPacket entryPacket) {
        return value;
    }
}
