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

/*******************************************************************************
 * Copyright (c) 2010 GigaSpaces Technologies Ltd. All rights reserved
 *
 * The software source code is proprietary and confidential information of GigaSpaces. You may use
 * the software source code solely under the terms and limitations of The license agreement granted
 * to you by GigaSpaces.
 *******************************************************************************/

import com.gigaspaces.internal.collections.CollectionsFactory;
import com.gigaspaces.internal.collections.IntegerSet;
import com.j_spaces.core.cache.context.Context;
import com.j_spaces.core.sadapter.SAException;
import com.j_spaces.kernel.IStoredList;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * List of lists that should return result after intersection.
 *
 * @author Yechiel
 * @version 1.0
 * @since 10.0
 */

@com.gigaspaces.api.InternalApi
public class MultiIntersectedStoredList<T>
        implements IScanListIterator<T>

{
    private static final int INTERSECTED_SIZE_LIMIT = 20000; //when reached vector will be dropped


    private IObjectsList _shortest;   //the base for scan
    private List<IObjectsList> _otherLists;
    private IScanListIterator<T> _current;
    private final boolean _fifoScan;
    private IntegerSet _intersectedSoFarFilter;
    private Set<Object> _intersectedSoFarSet;
    private boolean _terminated;
    private boolean _started;
    private final IObjectsList _allElementslist;
    private final boolean _falsePositiveFilterOnly;
    private final Context _context;
    private Set _uniqueLists;
    private final boolean _alternatingThread;
    private final AtomicInteger _alternatingThreadBarrier; //pass thru volatile


    public MultiIntersectedStoredList(Context context, IObjectsList list, boolean fifoScan, IObjectsList allElementslist, boolean falsePositiveFilterOnly) {
        this(context,  list,  fifoScan,  allElementslist, falsePositiveFilterOnly,false);
    }
    public MultiIntersectedStoredList(Context context, IObjectsList list, boolean fifoScan, IObjectsList allElementslist, boolean falsePositiveFilterOnly,boolean alternatingThread) {
        _fifoScan = fifoScan;
        _allElementslist = allElementslist;
        _shortest = list != allElementslist ? list : null;
        _falsePositiveFilterOnly = falsePositiveFilterOnly;
        _context = context;
        _alternatingThread = alternatingThread;
        if (_alternatingThread) {
            _alternatingThreadBarrier = new AtomicInteger(0);
            _alternatingThreadBarrier.incrementAndGet(); //set to 1 bypass jvm optimizations
        }
        else
            _alternatingThreadBarrier = null;
    }


    public void add(IObjectsList list, boolean shortest) {
        if (_alternatingThreadBarrier != null && _alternatingThreadBarrier.get() == 0)
            throw new RuntimeException("internal error alternating thread");
        try {
            if (list == null || list == _allElementslist || (list == _shortest))
                return;
            boolean duplicate = isDuplicate(list);
            if (duplicate && !shortest)
                return;  //already in
            boolean added = true;
            try {
                if (shortest) {
                    if (_shortest != null) {
                        added = addToOtherLists(_shortest);
                        if(duplicate){
                            _otherLists.remove(list);
                        }
                    }
                    _shortest = list;
                } else
                    added = addToOtherLists(list);
            } finally {
                if (added && !duplicate)
                    _uniqueLists.add(list);
            }
        }
        finally
        {
            if (_alternatingThreadBarrier != null )
                _alternatingThreadBarrier.incrementAndGet(); //set to 1 bypass jvm optimizations
        }
    }

    private boolean isDuplicate(IObjectsList list) {
        if (_uniqueLists == null) {
            _uniqueLists = new HashSet();
            if (_shortest != null)
                _uniqueLists.add(_shortest);
        }
        return
                (_uniqueLists.contains(list));

    }

    private boolean addToOtherLists(IObjectsList list) {
        if (!list.isIterator() && ((IStoredList<T>) list).size() > INTERSECTED_SIZE_LIMIT) {
            _context.setBlobStoreUsePureIndexesAccess(false);
            return false;
        }
        if (_otherLists == null)
            _otherLists = new ArrayList<IObjectsList>(2);
        _otherLists.add(list);
        return true;
    }

    @Override
    public boolean hasNext() {
        if (_alternatingThreadBarrier != null && _alternatingThreadBarrier.get() == 0)
            //a dummy check just to go thru volatile barrier
            throw new RuntimeException("internal error alternating thread");
        try {
            try {
                if (_terminated)
                    return false;

                if (!_started) {//intersect
                    _started = true;
                    prepareForListsIntersection();
                    if (_terminated)
                        return false;  //nothing to return
                    _current = prepareListIterator(_shortest);
                }
                if (_current != null && _current.hasNext())
                    return true;
                _current = null;
                return false;
            } catch (SAException ex) {
            } //never happens
            return false;
        }
        finally
        {
            if (_alternatingThreadBarrier != null )
                _alternatingThreadBarrier.incrementAndGet(); //set to 1 bypass jvm optimizations
        }
    }


    private void prepareForListsIntersection() {
        if (_shortest == null)
            throw new RuntimeException("shortest list is null !!!!!!!");

        if (_otherLists != null && _otherLists.contains(_shortest))
            _otherLists.remove(_shortest);
        if (_otherLists == null || _otherLists.isEmpty())
            return;
        for (int i = 0; i < _otherLists.size(); i++) {
            intersectList(_otherLists.get(i), i == 0);
            if ((_intersectedSoFarFilter != null && _intersectedSoFarFilter.isEmpty()) || (_intersectedSoFarSet != null && _intersectedSoFarSet.isEmpty())) {
                _terminated = true;
                break; //nothing left
            }
        }
    }

    private void intersectList(IObjectsList list, boolean isFirstIndex) {
        if (_falsePositiveFilterOnly)
            intersectListFilter(list, isFirstIndex);
        else
            intersectListSet(list, isFirstIndex);
    }

    private void intersectListFilter(IObjectsList list, boolean isFirstIndex) {
        IntegerSet newIntersectedIndices = (_intersectedSoFarFilter != null && !_intersectedSoFarFilter.isEmpty())
                ? CollectionsFactory.getInstance().createIntegerSet(_intersectedSoFarFilter.size())
                : CollectionsFactory.getInstance().createIntegerSet();

        IScanListIterator<T> toScan = prepareListIterator(list);
        try {
            int sofar = 0;
            boolean overflow = false;

            while (toScan.hasNext()) {
                T el = toScan.next();
                if (isFirstIndex || _intersectedSoFarFilter == null || _intersectedSoFarFilter.contains(System.identityHashCode(el))) {
                    newIntersectedIndices.add(System.identityHashCode(el));
                }
                if (++sofar > INTERSECTED_SIZE_LIMIT) {
                    overflow = true;
                    _context.setBlobStoreUsePureIndexesAccess(false);
                    break;
                }
            }

            if (!overflow)
                _intersectedSoFarFilter = newIntersectedIndices;

            if (toScan != null)
                toScan.releaseScan();
        } catch (SAException ex) {
            throw new RuntimeException(ex);
        }
    }

    private void intersectListSet(IObjectsList list, boolean isFirstIndex) {
        Set<Object> newIntersectedIndices = (_intersectedSoFarSet != null && !_intersectedSoFarSet.isEmpty()) ? new HashSet<Object>(_intersectedSoFarSet.size()) : new HashSet();

        IScanListIterator<T> toScan = prepareListIterator(list);
        try {
            int sofar = 0;
            boolean overflow = false;

            while (toScan.hasNext()) {
                T el = toScan.next();
                if (isFirstIndex || _intersectedSoFarSet.contains(el)) {
                    newIntersectedIndices.add(el);
                }
                if (++sofar > INTERSECTED_SIZE_LIMIT) {
                    overflow = true;
                    _context.setBlobStoreUsePureIndexesAccess(false);
                    break;
                }
            }

            if (!overflow)
                _intersectedSoFarSet = newIntersectedIndices;
            if (toScan != null)
                toScan.releaseScan();
        } catch (SAException ex) {
            throw new RuntimeException(ex);
        }
    }


    @Override
    public T next() {
        if (_alternatingThreadBarrier != null && _alternatingThreadBarrier.get() == 0)
            //a dummy check just to go thru volatile barrier
            throw new RuntimeException("internal error alternating thread");
        try {
            T res = null;
            try {
                if (!_terminated)
                    res = getNext();
            } catch (SAException ex) {
            } //never happens
            return res;
    }
    finally
    {
        if (_alternatingThreadBarrier != null )
            _alternatingThreadBarrier.incrementAndGet(); //set to 1 bypass jvm optimizations
    }
    }

    private T getNext() throws SAException {
        T res = null;
        do {
            res = _current.next();
            if (_falsePositiveFilterOnly) {
                if (res != null && _intersectedSoFarFilter != null && !_intersectedSoFarFilter.contains(System.identityHashCode(res))) {
                        res = null;
                        continue;
                    }
            } else {
                if (res != null && _intersectedSoFarSet != null && !_intersectedSoFarSet.contains(res)) {
                        res = null;
                        continue;
                    }
                }
            if (res != null)
                break;
            }
        while (_current.hasNext());

        if (res == null)
            _terminated = true;
        return res;
    }

    @Override
    public void releaseScan() {
        try {
            if (_current != null) {
                _current.releaseScan();
                _current = null;
            }
        } catch (SAException ex) {
        } //never happens
    }

    @Override
    public int getAlreadyMatchedFixedPropertyIndexPos() {
        return -1;
    }

    @Override
    public boolean isAlreadyMatched() {
        return false;
    }

    @Override
    public boolean isIterator() {
        return true;
    }

    protected IScanListIterator<T> prepareListIterator(IObjectsList list) {
        return (!list.isIterator()) ? new ScanSingleListIterator((IStoredList<T>) list, _fifoScan) : (IScanListIterator<T>) list;

    }

    public boolean isMultiList() {
        return _shortest != null && getNumOfLists() > 1;
    }

    private int getNumOfLists() {
        return _otherLists != null ? _otherLists.size() + 1 : 1;
    }

    /**
     * does this iter contain multiple lists
     */
    @Override
    public boolean isMultiListsIterator()
    {
        return true;
    }

    /**
     * create a shallow copy ready for alternating thread usage
     * @return a new shallow copyed IScanListIterator ready for alternating thread usage
     * NOTE!! should be called before first hasNext() call
     *
     */
    @Override
    public MultiIntersectedStoredList createCopyForAlternatingThread()
    /* NOTE!! should be called before first hasNext() call*/
    {
        if (_alternatingThread)
            throw new RuntimeException("internal error-original multiIntersectedlist is already set for alternate thread");
        MultiIntersectedStoredList newIter = new MultiIntersectedStoredList(_context,  _shortest, _fifoScan,  _allElementslist, _falsePositiveFilterOnly,true);
        newIter._shortest = _shortest;
        newIter._uniqueLists = _uniqueLists;
        newIter._otherLists = _otherLists;
        newIter._alternatingThreadBarrier.incrementAndGet();
        return newIter;
    }
}
