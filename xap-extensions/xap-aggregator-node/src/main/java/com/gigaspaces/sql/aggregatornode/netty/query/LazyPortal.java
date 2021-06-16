package com.gigaspaces.sql.aggregatornode.netty.query;

import com.gigaspaces.sql.aggregatornode.netty.exception.ProtocolException;

@FunctionalInterface
public interface LazyPortal<T> {
    Portal<T> getPortal() throws ProtocolException;
}
