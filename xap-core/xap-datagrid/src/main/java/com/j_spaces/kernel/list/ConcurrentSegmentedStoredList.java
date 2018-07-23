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


package com.j_spaces.kernel.list;

import com.gigaspaces.internal.utils.concurrent.UncheckedAtomicIntegerFieldUpdater;
import com.gigaspaces.internal.utils.threadlocal.AbstractResource;
import com.gigaspaces.internal.utils.threadlocal.PoolFactory;
import com.gigaspaces.internal.utils.threadlocal.ThreadLocalPool;
import com.j_spaces.kernel.IObjectInfo;
import com.j_spaces.kernel.IStoredList;
import com.j_spaces.kernel.IStoredListIterator;
import com.j_spaces.kernel.SystemProperties;
import com.j_spaces.kernel.list.StoredListChainSegment.ConcurrentSLObjectInfo;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.LongAdder;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Yechiel Fefer
 * @version 1.0
 * @since 7.1
 */
@com.gigaspaces.api.InternalApi
public class ConcurrentSegmentedStoredList<T>
        extends ConcurrentStoredList<T> {
    /**
     * concurrent segmented stored list, implementation is nearly concurrent ordered operations are
     * not supported since there is only one insertion point
     */
    /* array of segments. only head is kept per segment*/
    final private StoredListChainSegment<T>[] _segments;
    final private LongAdder  _listSize;



    public ConcurrentSegmentedStoredList(boolean supportFifoPerSegment,int inputNumOfSegments,boolean padded) {


        super(true /*segmented*/,  supportFifoPerSegment);
        int numOfSegments =inputNumOfSegments;
        if (numOfSegments == 0)
         numOfSegments = Integer.getInteger(SystemProperties.ENGINE_STORED_LIST_SEGMENTS, SystemProperties.ENGINE_STORED_LIST_SEGMENTS_DEFAULT);
        // if set to 0 - use the default
        if (numOfSegments == 0)
            numOfSegments = SystemProperties.ENGINE_STORED_LIST_SEGMENTS_DEFAULT;

        _segments = new StoredListChainSegment[numOfSegments];

        //create segments & locks
        for (int seg = 0; seg < numOfSegments; seg++) {
            _segments[seg] = new StoredListChainSegment<T>((short) seg, supportFifoPerSegment,padded);
        }
        _listSize = padded ? new LongAdder() : null;

    }



    @Override
    public int size()
    {
        if (_listSize == null) //not padded
            return super.size();
        long res = _listSize.longValue();
        return res < 0 ? 0 : (int)res;
    }

    /**
     * get the number of segments in this SL
     */
    protected int getNumSegments() {
        return _segments.length;
    }

    protected StoredListChainSegment<T> getSegment(int seg)
    {
        return _segments[seg];
    }

    @Override
    protected int incremenetAndGetSize()
    {
        if (_listSize ==null)
            return super.incremenetAndGetSize();
        incrementSize();
        return
                size();
    }
    @Override
    protected void incrementSize()

    {
        if (_listSize ==null)
            super.incrementSize();
        else
            _listSize.increment();
    }
    @Override
    protected void decrementSize()
    {
        if (_listSize ==null)
            super.decrementSize();
        else
            _listSize.decrement();
    }

    @Override
    public boolean invalidate() {
        throw new UnsupportedOperationException(); //supported only for a single segment list
    }

    @Override
    protected boolean supportsInvalidation()
    {
        return false;
    }

}
