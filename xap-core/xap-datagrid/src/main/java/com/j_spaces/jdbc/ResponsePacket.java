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

package com.j_spaces.jdbc;

import com.gigaspaces.internal.transport.IEntryPacket;

import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;

/**
 * The ResponsePacket class. Every response to the JDBC driver is wrapped in this class. it can
 * contain an SQLException if one occurred, an integer result, that represents the number of updates
 * made, or 0, in case of other type of queries, or a ResultEntry as a response to a select.
 *
 * @author Michael Mitrani - 2Train4, 2004
 */
@com.gigaspaces.api.InternalApi
public class ResponsePacket implements Serializable, Iterable<IEntryPacket> {


    /**
     *
     */
    private static final long serialVersionUID = 1L;

    private int intResult = -1;
    private ResultEntry resultEntry = null;
    private Collection<IEntryPacket> resultSet = null;
    private IEntryPacket[] resultArray;

    public ResponsePacket() {
    }

    public int getIntResult() {
        return intResult;
    }

    public void setIntResult(int intResult) {
        this.intResult = intResult;
    }

    public ResultEntry getResultEntry() {
        return resultEntry;
    }

    public void setResultSet(Collection<IEntryPacket> resultSet) {
        this.resultSet = resultSet;
    }

    public void setResultEntry(ResultEntry resultEntry) {
        this.resultEntry = resultEntry;
    }

    public Collection<IEntryPacket> getResultSet() {
        return this.resultSet;
    }

    @Override
    public String toString() {

        if (intResult != -1)
            return String.valueOf(intResult);
        if (resultEntry != null)
            return resultEntry.toString();
        return null;
    }


    /**
     * @param resultArray the resultArray to set
     */
    public void setResultArray(IEntryPacket[] resultArray) {
        this.resultArray = resultArray;
    }


    public Object getFirst() {
        if (resultEntry != null)
            return resultEntry.getFieldValues()[0][0];

        if (resultArray != null)
            return resultArray[0];

        return resultSet.iterator().next();


    }

    /**
     * @return IEntryPacket array
     */
    public IEntryPacket[] getArray() {
        if (resultArray != null)
            return resultArray;

        IEntryPacket[] result = new IEntryPacket[resultSet.size()];

        return resultSet.toArray(result);

    }

    public int size() {
        if (resultEntry != null)
            return resultEntry.getRowNumber();

        if (resultArray != null)
            return resultArray.length;

        return resultSet.size();
    }

    /* (non-Javadoc)
     * @see java.lang.Iterable#iterator()
     */
    public Iterator<IEntryPacket> iterator() {
        if (resultArray != null)
            return new ArrayIterator<IEntryPacket>(resultArray);

        return resultSet.iterator();
    }

    private static class ArrayIterator<T> implements Iterator<T> {
        public T[] _arr;
        private int _iterIndex = 0;

        /**
         * @param resultArray
         */
        public ArrayIterator(T[] resultArray) {
            _arr = resultArray;
        }

        /* (non-Javadoc)
         * @see java.util.Iterator#hasNext()
         */
        public boolean hasNext() {
            return _iterIndex < _arr.length;
        }

        /* (non-Javadoc)
         * @see java.util.Iterator#next()
         */
        public T next() {
            return _arr[_iterIndex++];
        }

        /* (non-Javadoc)
         * @see java.util.Iterator#remove()
         */
        public void remove() {

        }

    }
}
