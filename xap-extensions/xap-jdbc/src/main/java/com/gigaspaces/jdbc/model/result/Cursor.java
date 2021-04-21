package com.gigaspaces.jdbc.model.result;

public interface Cursor<T> {
    enum Type {SCAN, HASH}

    boolean next();

    T getCurrent();

    void reset();

    boolean isBeforeFirst();
}
