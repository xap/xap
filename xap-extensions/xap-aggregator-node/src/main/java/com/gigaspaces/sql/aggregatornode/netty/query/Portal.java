package com.gigaspaces.sql.aggregatornode.netty.query;

import java.util.Iterator;

public interface Portal<T> extends Iterator<T>, AutoCloseable {
    /**
     * Operation tag
     */
    enum Tag {
        SELECT, DELETE, UPDATE, INSERT, NONE
    }

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
    Tag tag();

    /**
     * @return Processed rows by operation.
     */
    int processed();
}
