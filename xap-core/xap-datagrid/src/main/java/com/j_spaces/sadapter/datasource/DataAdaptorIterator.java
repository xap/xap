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

package com.j_spaces.sadapter.datasource;

import com.gigaspaces.datasource.DataIterator;
import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.internal.server.space.metadata.SpaceTypeManager;
import com.gigaspaces.internal.server.storage.EntryDataType;
import com.gigaspaces.internal.server.storage.EntryHolderFactory;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.j_spaces.core.sadapter.ISAdapterIterator;
import com.j_spaces.core.sadapter.SAException;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by IntelliJ IDEA. User: assafr Date: Apr 24, 2008 Time: 11:41:20 AM To change this
 * template use File | Settings | File Templates.
 */
@com.gigaspaces.api.InternalApi
public class DataAdaptorIterator implements ISAdapterIterator<IEntryHolder> {
    private final static Logger _logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_PERSISTENT);

    private final SpaceTypeManager _typeManager;
    private final EntryDataType _entryDataType;

    // a list of iterators (excluding the first).
    private List<Iterator<IEntryPacket>> _listOfIterators = new LinkedList<Iterator<IEntryPacket>>();

    // current iterator; initialized with first iterator.
    private Iterator<IEntryPacket> _iterator;

    // cursor to current iterator in list; initialized to first in list
    private int _cursor = 1;

    public DataAdaptorIterator(SpaceTypeManager typeManager, EntryDataType entryDataType) {
        this._typeManager = typeManager;
        this._entryDataType = entryDataType;
    }

    /**
     * Appends the specified iterator to the end of this list.
     *
     * @param iterator iterator of {@link com.j_spaces.core.IGSEntry} to be appended to this list.
     */
    public void add(Iterator<IEntryPacket> iterator) {
        //discard null input
        if (iterator == null)
            return;

        if (_iterator == null)
            _iterator = iterator;    //initialize with the first

        _listOfIterators.add(iterator);
    }

    /**
     * Returns the next element in the iteration.  Calling this method repeatedly until the {@link
     * #hasNext()} method returns false will return each element in the underlying collection
     * exactly once.
     *
     * @return the next element in the iteration.
     * @throws NoSuchElementException iteration has no more elements.
     */
    private IEntryPacket _next() {
        return _iterator.next();
    }

    /**
     * Returns <tt>true</tt> if the iteration has more elements. (In other words, returns
     * <tt>true</tt> if <tt>_next</tt> would return an element rather than throwing an exception.)
     *
     * @return <tt>true</tt> if the iterator has more elements.
     */
    private boolean _hasNext() {
        try {
            if (_iterator.hasNext())
                return true;
            else
                while (_cursor < _listOfIterators.size()) {
                    _iterator = _listOfIterators.get(_cursor++); //advance to next
                    if (_iterator.hasNext())
                        return true;
                }

            return false;

        } catch (NullPointerException npe) {
            if (_iterator == null)
                return false;    //supplied iterator/s was null
            else
                throw npe;    //throw if thrown from within the iterator
        }
    }

    /**
     * @return next iterated object (instance of IEntryHolder)
     * @see com.j_spaces.core.sadapter.ISAdapterIterator#next()
     */
    public IEntryHolder next() throws SAException {
        if (!_hasNext())
            return null;

        try {
            IEntryPacket next = _next();

            if (next == null)
                return null;
            IServerTypeDesc typeDesc = _typeManager.loadServerTypeDesc(next);

            if (_logger.isDebugEnabled())
                _logger.debug("CacheIterator#next() RETURNs " + next.toString());

            return EntryHolderFactory.createEntryHolder(typeDesc, next, _entryDataType);
        } catch (Throwable t) {
            //could be UnusableEntryException, UnknownTypeException or any other unexpected exception
            throw new SAException(t);
        }
    }

    /**
     * Closes this iterator. Note: subsequent calls will throw {@link NullPointerException}
     *
     * @see com.j_spaces.core.sadapter.ISAdapterIterator#close()
     */
    public void close() throws SAException {
        //close excess resources
        Iterator<Iterator<IEntryPacket>> iteration = _listOfIterators.listIterator();
        while (iteration.hasNext()) {
            Iterator<IEntryPacket> iterator = iteration.next();
            discardOfIterator(iterator);

            iteration.remove(); //remove from iteration - to be collected by gc.
        }
        //help gc
        _listOfIterators = null;
        _iterator = null;
    }

    /**
     * Discards of the current Iterator. <p> Note: Iterator can be an instance of java.util.Iterator
     * - thus requiring a type check. This is due to the Map Iterator returned by loadAll, which is
     * passed to {@link #add(Iterator)}.
     */
    private void discardOfIterator(Iterator<IEntryPacket> iterator) throws SAException {
        try {
            if (iterator instanceof DataIterator)
                ((DataIterator) iterator).close();
        } catch (Throwable t) {
            throw new SAException(t);
        }
    }

    public static String getDataSourceShareIteratorTTLDefault(long leaseManagerExpirationTimeRecentDeletes, long leaseManagerExpirationTimeRecentUpdates) {
        return String.valueOf(Math.min(Math.min(leaseManagerExpirationTimeRecentDeletes, leaseManagerExpirationTimeRecentUpdates) / 2, 10000));
    }
}
