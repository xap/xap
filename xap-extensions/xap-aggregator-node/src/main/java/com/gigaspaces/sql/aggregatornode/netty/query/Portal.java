package com.gigaspaces.sql.aggregatornode.netty.query;

import com.gigaspaces.sql.aggregatornode.netty.exception.ProtocolException;

import java.util.Iterator;

public interface Portal<T> extends Iterator<T>, AutoCloseable {
    /**
     * @return Portal name.
     */
    String name();

    /**
     * @return Bounded statement name.
     */
    Statement getStatement();

    /**
     * @return Row description.
     */
    RowDescription getDescription();

    /**
     * @return Operation tag.
     */
    String tag();

    /**
     * Executes portal operation
     */
    void execute() throws ProtocolException;

    /**
     * Indicates whether the portal is empty.
     */
    default boolean empty() {
        return false;
    }
}
