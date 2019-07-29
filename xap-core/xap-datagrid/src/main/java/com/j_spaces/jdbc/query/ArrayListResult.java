/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 *
 */
package com.j_spaces.jdbc.query;

import com.gigaspaces.internal.transport.IEntryPacket;
import com.j_spaces.jdbc.SelectColumn;

import java.util.ArrayList;
import java.util.Collection;


/**
 * @author anna
 * @since 7.1
 */
@com.gigaspaces.api.InternalApi
public class ArrayListResult
        extends ArrayList<IEntryPacket>
        implements IQueryResultSet<IEntryPacket> {
    /**
     *
     */
    private static final long serialVersionUID = 1L;

    /**
     *
     */
    public ArrayListResult() {
        super();
    }

    public ArrayListResult(IEntryPacket[] array) {
        super(array.length); // capacity set to the size of the array
        for (IEntryPacket entryPacket : array) {
            add(entryPacket);
        }
    }

    public ArrayListResult(IEntryPacket entryPacket) {
        super(1); // capacity set to 1 since only packet is going to be in the list
        add(entryPacket);

    }

    public ArrayListResult(Collection<IEntryPacket> entries) {
        super(entries);
    }

    /*
     * (non-Javadoc)
     * @see
     * com.j_spaces.jdbc.query.IQueryResultSet#getFieldValue(com.j_spaces.jdbc
     * .SelectColumn, com.gigaspaces.internal.transport.IEntryPacket)
     */
    public Object getFieldValue(SelectColumn column, IEntryPacket entry) {
        return column.getFieldValue(entry);
    }

    /*
     * (non-Javadoc)
     * @see com.j_spaces.jdbc.query.IQueryResultSet#newResultSet()
     */
    public IQueryResultSet<IEntryPacket> newResultSet() {
        return new ArrayListResult();
    }

    @Override
    public IQueryResultSet<IEntryPacket> newResultSet(Collection<IEntryPacket> entries) {
        return new ArrayListResult(entries);
    }

    /**
     * Return an intersection of this set and a given one.
     *
     * @return the intersection
     */
    public IQueryResultSet<IEntryPacket> intersect(
            IQueryResultSet<IEntryPacket> set) {

        this.retainAll(set);
        return this;
    }

    /**
     * Return a union of this set and a given one
     *
     * @return the union set
     */
    public IQueryResultSet<IEntryPacket> union(IQueryResultSet<IEntryPacket> set) {
        this.removeAll(set);
        this.addAll(set);
        return this;
    }


}
