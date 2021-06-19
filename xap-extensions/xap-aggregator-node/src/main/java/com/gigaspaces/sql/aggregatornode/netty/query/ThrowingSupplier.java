package com.gigaspaces.sql.aggregatornode.netty.query;

@FunctionalInterface
public interface ThrowingSupplier<T, E extends Exception> {
    T apply() throws E;
}
