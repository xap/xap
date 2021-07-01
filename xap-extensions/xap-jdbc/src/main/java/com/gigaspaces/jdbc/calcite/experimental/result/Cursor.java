package com.gigaspaces.jdbc.calcite.experimental.result;

public interface Cursor<T> {
    enum Type {SCAN, HASH}

    boolean next();

    T getCurrent();

    void reset();

    boolean isBeforeFirst();
}
