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

import com.j_spaces.core.sadapter.SAException;
import com.j_spaces.kernel.ICollection;
import com.j_spaces.kernel.IStoredList;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * List of stored lists. Used to create a union of several lists. For example for matching a
 * collection of values. Note:The list doesn't support multithreaded.
 *
 * @author anna
 * @version 1.0
 * @since 8.0
 */
@com.gigaspaces.api.InternalApi
public class MultiStoredList<T>
        implements IScanListIterator<T> {

    private final List<IObjectsList> _multiList;
    private IScanListIterator<T> _current;
    private final boolean _fifoScan;
    private int _posInMultlist = -1;
    private Set _uniqueLists;
    private final boolean _alternatingThread;
    private final AtomicInteger _alternatingThreadBarrier; //pass thru volatile

    public MultiStoredList() {
        this(null, false);
    }

    public MultiStoredList(List<IObjectsList> multiList, boolean fifoScan) {
        this(multiList, fifoScan,false);
    }

    public MultiStoredList(List<IObjectsList> multiList, boolean fifoScan,boolean alternatingThread) {
        if (multiList == null)
            _multiList = new LinkedList<IObjectsList>();
        else {
            if (multiList.size() > 1)
            {
                List<IObjectsList> ml = new LinkedList<IObjectsList>();
                _uniqueLists = new HashSet();
                for (IObjectsList o : multiList)
                {
                    if (_uniqueLists.add(o))
                        ml.add(o);
                }
                multiList = ml;
            }
            _multiList = multiList;
        }
        _fifoScan = fifoScan;
        _alternatingThread = alternatingThread;
        if (_alternatingThread) {
            _alternatingThreadBarrier = new AtomicInteger(0);
            _alternatingThreadBarrier.incrementAndGet(); //set to 1 bypass jvm optimizations
        }
        else
            _alternatingThreadBarrier = null;
    }

    public void add(IObjectsList l) {
        if (_alternatingThreadBarrier != null && _alternatingThreadBarrier.get() == 0)
            throw new RuntimeException("internal error alternating thread");
        try {
            if (l == null)
                return;
            if (_uniqueLists ==null)
            {
                _uniqueLists = new HashSet();
                if (!_multiList.isEmpty())
                    _uniqueLists.addAll(_multiList);
            }

            if (_uniqueLists.add(l))
                _multiList.add(l);

        }
        finally
        {
            if (_alternatingThreadBarrier != null )
                _alternatingThreadBarrier.incrementAndGet(); //set to 1 bypass jvm optimizations
        }
    }


    public boolean hasNext() {
        if (_alternatingThreadBarrier != null && _alternatingThreadBarrier.get() == 0)
            //a dummy check just to go thru volatile barrier
            throw new RuntimeException("internal error alternating thread");
        try {
            while (true) {
                if (_current == null && _posInMultlist >= _multiList.size() - 1) {
                    return false;
                }
                if (_current != null && _current.hasNext())
                    return true;
                _current = null;
                if (_posInMultlist < _multiList.size() - 1) {
                    IObjectsList current = _multiList.get(++_posInMultlist);
                    _current = prepareListIterator(current);
                }
            }
        } catch (SAException ex) {
            return false;
        } //never happens
        finally
        {
            if (_alternatingThreadBarrier != null )
                _alternatingThreadBarrier.incrementAndGet(); //set to 1 bypass jvm optimizations
        }
    }


    public T next() {
        if (_alternatingThreadBarrier != null && _alternatingThreadBarrier.get() == 0)
            //a dummy check just to go thru volatile barrier
            throw new RuntimeException("internal error alternating thread");
        T res = null;
        try {
            res = _current.next();
            return res;
        } catch (SAException ex) {
            return res;
        } //never happens
        finally
        {
            if (_alternatingThreadBarrier != null )
                _alternatingThreadBarrier.incrementAndGet(); //set to 1 bypass jvm optimizations
        }
    }

    public void remove() {
        throw new UnsupportedOperationException();
    }


    public void releaseScan() {
        try {
            if (_current != null)
                _current.releaseScan();
        } catch (SAException ex) {
        } //never happens
    }

    public int getAlreadyMatchedFixedPropertyIndexPos() {
        return -1;
    }

    public boolean isAlreadyMatched() {
        return false;
    }

    public boolean isIterator() {
        return true;
    }

    protected IScanListIterator<T> prepareListIterator(IObjectsList list) {
        if (!list.isIterator())
        {
            if (_alternatingThread)
                return new ScanSingleListIterator((IStoredList<T>) list, _fifoScan,true);
            else
                return new ScanSingleListIterator((IStoredList<T>) list, _fifoScan);
        }
        else
            return (IScanListIterator<T>) list;

    }

    protected IScanListIterator<T> getCurrentList() {
        return _current;
    }

    public List<IObjectsList> getAllLists() {
        return _multiList;
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
     */
    @Override
    public MultiStoredList createCopyForAlternatingThread()
    // NOTE!! should be called before first hasNext() call
    {
        if (_alternatingThread)
            throw new RuntimeException("internal error-original multilist is already set for alternate thread");
        MultiStoredList newIter = new MultiStoredList(_multiList, _fifoScan,true);
        newIter._uniqueLists = _uniqueLists;
        newIter._alternatingThreadBarrier.incrementAndGet();
        return newIter;
    }


    @Override
    public int hashCode()
    {//NOTE- meanningfull only when all lists have been added to the MultiSL
        if (_multiList.isEmpty())
            return 0;
        int res = Integer.MIN_VALUE;
        for (Object o : _multiList)
            res = res < o.hashCode() ? o.hashCode() : res;
        return res;
    }

    @Override
    public boolean equals(Object o)
    {//NOTE- meanningfull only when all lists have been added to the MultiSL
        if (o == this)
            return true;
        if (!(o instanceof MultiStoredList))
            return false;
        MultiStoredList other = (MultiStoredList)o;
        if (_multiList.size() != other.getAllLists().size())
            return false;
        Set e = new HashSet(_multiList);
        for (Object l : other.getAllLists())
            if (!e.contains(o))
                return false;
        return true;
    }

    @Override
    public boolean hasSize() {
        for (IObjectsList next : _multiList) {
            if (!(next instanceof ICollection)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int size() {
        int size = 0;
        for (IObjectsList next : _multiList) {
            if (next instanceof ICollection) {
                size += ((ICollection) next).size();
            } else {
                return Integer.MAX_VALUE;
            }
        }
        return size;
    }
}
