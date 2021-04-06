package com.gigaspaces.jdbc.model.result;

public interface Cursor<T> {
    boolean next();

    T getCurrent();

    void reset();

    boolean isBeforeFirst();
}
