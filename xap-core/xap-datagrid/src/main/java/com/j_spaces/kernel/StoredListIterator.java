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

package com.j_spaces.kernel;

import com.gigaspaces.internal.utils.threadlocal.AbstractResource;
import com.gigaspaces.internal.utils.threadlocal.PoolFactory;
import com.j_spaces.kernel.AbstractStoredList.ObjectInfo;

/**
 * Iterates the data in the StoredList
 *
 * @author anna
 * @since 6.1
 */
final public class StoredListIterator<T>
        extends AbstractResource
        implements IStoredListIterator<T> {

    public static class StoredListIteratorFactory implements PoolFactory<StoredListIterator> {
        public StoredListIterator create() {
            return new StoredListIterator();
        }
    }

    /**
     * This class is used in order to return and request (during scan) information from the list-
     * the subject is returned in separate field in order to to need synchronized access to the
     * m_ObjectInfo field that may be changed by other threads
     */

    private ObjectInfo<T> _currentOI; // current List basic unit
    private ObjectInfo<T> _pivotOI;   // scan pivot
    private T _subject;   // stored object
    private boolean _tryFwdScan; // tail to head (lifo),true = in fwd direction, valid in random scan
    private boolean _backward;  //true - from head to tail (fifo)
    private int _scanLimit; //avoid perpetual scan situation
    private final boolean _alternatingThread;

    public StoredListIterator() {
        this(false);
    }
    public StoredListIterator(boolean alternatingThread) {
        _alternatingThread = alternatingThread;
    }

    @Override
    protected void clean() {
        setCurrentOI(null);
        setPivotOI(null);
        setSubject(null);
        setTryForwardScan(false);
        setBackwardsScan(false);
        setScanLimit(0);
    }

    /**
     * @param currentOI The currentOI to set.
     */
    public void setCurrentOI(ObjectInfo<T> currentOI) {
        this._currentOI = currentOI;
    }

    /**
     * @return Returns the currentOI.
     */
    public ObjectInfo<T> getCurrentOI() {
        return _currentOI;
    }

    /**
     * @param pivotOI The pivotOI to set.
     */
    public void setPivotOI(ObjectInfo<T> pivotOI) {
        this._pivotOI = pivotOI;
    }

    /**
     * @return Returns the pivotOI.
     */
    public ObjectInfo<T> getPivotOI() {
        return _pivotOI;
    }

    /**
     * @param subject The subject to set.
     */
    public void setSubject(T subject) {
        this._subject = subject;
    }

    /**
     * @return Returns the subject.
     */
    public T getSubject() {
        return _subject;
    }

    /**
     * @param tryFwdScan The tryFwdScan to set.
     */
    public void setTryForwardScan(boolean tryFwdScan) {
        this._tryFwdScan = tryFwdScan;
    }

    /**
     * @return Returns the tryFwdScan.
     */
    public boolean tryForwardScan() {
        return _tryFwdScan;
    }

    /**
     * @param backward The backward to set.
     */
    public void setBackwardsScan(boolean backward) {
        this._backward = backward;
    }

    /**
     * @return Returns the backward.
     */
    public boolean backwardsScan() {
        return _backward;
    }

    /**
     * @param scanLimit The scanLimit to set.
     */
    public int setScanLimit(int scanLimit) {
        return this._scanLimit = scanLimit;
    }

    /**
     * @return Returns the scanLimit.
     */
    public int getScanLimit() {
        return _scanLimit;
    }

    public boolean isAlternatingThread()
    {
        return _alternatingThread;
    }

    @Override
    public void release() {
        if (_alternatingThread)
            return;
        super.release();
    }


}