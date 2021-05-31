package com.gigaspaces.internal.server.space.tiered_storage;

public class SingleQueryParameter {
    final private String column;
    final private Object value;
    final private Class<?> type;

    public SingleQueryParameter(String column, Object value, Class<?> type) {
        this.column = column;
        this.value = value;
        this.type = type;
    }

    public String getColumn() {
        return column;
    }

    public Object getValue() {
        return value;
    }

    public Class<?> getType() {
        return type;
    }
}