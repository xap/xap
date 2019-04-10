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
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Yechiel Fefer
 * @version 1.0
 * @since 7.1
 */
@com.gigaspaces.api.InternalApi
public class ConcurrentSegmentedStoredList<T>
        implements IStoredList<T> {
    /**
     * concurrent segmented stored list, implementation is nearly concurrent ordered operations are
     * not supported since there is only one insertion point
     */
    /* array of segments. only head is kept per segment*/
    final private StoredListChainSegment<T>[] _segments;
    private volatile int _size;

    final private static ThreadLocalPool<SegmentedListIterator> _SLHolderPool =
            new ThreadLocalPool<SegmentedListIterator>(new SegmentedListIteratorFactory());

    // counts the number of adds - used to spread the objects evenly between segments
    private int addCounter = 0;
    // counts the number of scans - used to spread the start between segments
    private int scanCounter = 0;

    private static final AtomicIntegerFieldUpdater<ConcurrentSegmentedStoredList> sizeUpdater = UncheckedAtomicIntegerFieldUpdater.newUpdater(ConcurrentSegmentedStoredList.class, "_size");


    public ConcurrentSegmentedStoredList(boolean segmented, boolean supportFifoPerSegment, int inputNumOfSegments,boolean padded) {
        int numOfSegments = Integer.getInteger(SystemProperties.ENGINE_STORED_LIST_SEGMENTS, SystemProperties.ENGINE_STORED_LIST_SEGMENTS_DEFAULT);
        if (!segmented) {
            numOfSegments = 1;
        } else {
            if (inputNumOfSegments > 0)
                numOfSegments = inputNumOfSegments;
        }
        // if set to 0 - use the default
        if (numOfSegments == 0)
            numOfSegments = SystemProperties.ENGINE_STORED_LIST_SEGMENTS_DEFAULT;

        _segments = new StoredListChainSegment[numOfSegments];

        //create segments & locks
        for (int seg = 0; seg < numOfSegments; seg++) {
            _segments[seg] = new StoredListChainSegment<T>((short) seg, supportFifoPerSegment,padded);
        }
    }

    public ConcurrentSegmentedStoredList(boolean segmented, boolean supportFifoPerSegment) {
        this(segmented, supportFifoPerSegment, 0, segmented /*multisegment ==> padded*/);
    }
    public ConcurrentSegmentedStoredList(boolean segmented, boolean supportFifoPerSegment, boolean padded) {
        this(segmented, supportFifoPerSegment, 0, padded );
    }

    public ConcurrentSegmentedStoredList(boolean segmented) {
        this(segmented, false /*supportFifoPerSegment*/, 0, segmented /*multisegment ==> padded*/);
    }

    public ConcurrentSegmentedStoredList(int inputNumOfSegments) {
        this(inputNumOfSegments > 1, false /*supportFifoPerSegment*/, inputNumOfSegments, inputNumOfSegments > 1 /*multisegment ==> padded*/);
    }


    public int size() {
        return _size;
    }

    /**
     * Returns true if the list is empty
     */
    public boolean isEmpty() {
        return size() == 0;
    }


    /**
     * store an element
     */
    public IObjectInfo<T> add(T subject) {
        return add(subject, null);

    }

    public IObjectInfo<T> add(T subject, Object segmentHint) {
        return addImpl(subject, segmentHint);
    }


    public IObjectInfo<T> addUnlocked(T subject) {
        return add(subject, null);

    }

    private boolean isSupportsFifoPerSegment()
    {
        return _segments[0].isSupportFifo();
    }

    private IObjectInfo<T> addImpl(T subject, Object segmentHint) {
        int res = sizeUpdater.incrementAndGet(this);
        if (res < 0)// list was invalidated
        {
            _size = Integer.MIN_VALUE;
            return null;
        }

        //select a segment to insert to
        int seg = 0;
        if(segmentHint == null || getNumSegments() == 1){
            seg = drawSegmentNumber(true /*add*/);
        } else {
            seg = gwtSegmentByHint(segmentHint);
        }

        StoredListChainSegment<T> segment = _segments[seg];
        return segment.add(subject);
    }

    private int gwtSegmentByHint(Object segmentHint) {
        return (Math.abs(segmentHint.hashCode())) % getNumSegments();
    }

    /**
     * remove an element described by ObjectInfo
     */
    public void remove(IObjectInfo<T> poi) {
        remove_impl(poi, true /*lock*/);
    }

    public void removeUnlocked(IObjectInfo<T> poi) {
        remove_impl(poi, false /*lock*/);
    }

    private void remove_impl(IObjectInfo<T> poi, boolean lock) {
        ConcurrentSLObjectInfo<T> oi = (ConcurrentSLObjectInfo<T>) poi;
        int seg = oi.getSegment();
        StoredListChainSegment<T> segment = _segments[seg];
        if (lock)
            segment.remove(oi);
        else
            segment.removeUnlocked(oi);
        sizeUpdater.decrementAndGet(this);

    }

    /**
     * is this object contained in the SL ?
     */
    public boolean contains(T obj) {
        throw new RuntimeException("ConcurrentSegmentedStoredList::contains not supported");
    }

    /**
     * given an object scan the list, find it and remove it, returns true if found
     */
    public boolean removeByObject(T obj) {
        if (getNumSegments() > 1)
            throw new RuntimeException("ConcurrentSegmentedStoredList::removeByObject not supported for multi segments list");
        if (_segments[0].removeByObject(obj)) {
            sizeUpdater.decrementAndGet(this);
            return true;
        }
        return false;
    }

    /**
     * Sets an indication that this StoredList is invalid.
     *
     * if {@linkplain #isEmpty() isEmpty()} returns true, the indication is set; otherwise the
     * indication remains false.
     *
     * Called by {@linkplain com.j_spaces.core.cache.PersistentGC PersistentGC} when scanning for
     * empty StoredList that can be garbage collected.
     *
     * @return <code>true</code> if StoredList was set to invalid; <code>false</code> otherwise.
     */
    public boolean invalidate() {
        return sizeUpdater.compareAndSet(this, 0, Integer.MIN_VALUE);
    }

    /**
     * Goes over all the segments and finds the "first" element. NOTE- if num of segments > 1 just
     * get according to segments order
     */
    public IObjectInfo<T> getHead() {
        IObjectInfo<T> res = null;
        for (int i = 0; i < getNumSegments(); i++) {
            res = _segments[i].getHead();
            if (res != null)
                break;
        }
        return res;
    }

    public T getObjectFromHead() {
        IObjectInfo<T> head = getHead();

        if (head == null)
            return null;

        return head.getSubject();
    }

    /**
     * return true if we can save iterator creation and get a single entry
     *
     * @return true if we can optimize
     */
    public boolean optimizeScanForSingleObject() {
        return getNumSegments() == 1 && isSupportsFifoPerSegment() && size() <= 1;
    }


    //draw a segment number for insertions/scans
    private int drawSegmentNumber(boolean add) {
        if (getNumSegments() == 1)
            return 0;
        int tnum = (int) Thread.currentThread().getId();
        if (tnum % getNumSegments() == 0)
            tnum++;
        return add ? Math.abs(((tnum * addCounter++) % getNumSegments())) : Math.abs(((tnum * scanCounter++) % getNumSegments()));
    }


    /**
     * establish a scan position. we select a random segment to start from
     */
    public IStoredListIterator<T> establishListScan(boolean randomScan) {
        if (!randomScan && ! isSupportsFifoPerSegment() )
            throw new RuntimeException("establishListScan non-random scans not supported");

        SegmentedListIterator<T> slh = _SLHolderPool.get();

        SegmentedListIterator<T> res = establishPos(slh, randomScan);

        if (res == null)
            slh.release();

        return res;
    }

    /**
     * establish a scan position- select a segment
     */
    private SegmentedListIterator<T> establishPos(SegmentedListIterator<T> res, boolean randomScan) {
        int startSegment = drawSegmentNumber(false /*add*/);
        res.setStartSegment((short) startSegment);
        res._scanLimit = size() * 5;
        res._randomScan = (randomScan && getNumSegments() == 1);

        for (int seg = startSegment, i = 0; i < getNumSegments(); i++, seg++) {
            if (seg == getNumSegments())
                seg = 0;
            res.setCurrentSegment((short) seg);
            StoredListChainSegment<T> segment = _segments[seg];
            if (segment.establishIterScanPos(res))
                return res;
        }
        return null; //all empty
    }

    /**
     * get the next element in scan order
     */
    public IStoredListIterator<T> next(IStoredListIterator<T> slh) {
        IStoredListIterator<T> slnext = nextPos((SegmentedListIterator<T>) slh);

        if (slnext == null)
            slh.release();

        return slnext;

    }

    private IStoredListIterator<T> nextPos(SegmentedListIterator<T> slh) {
        int startSegment = slh.getCurrentSegment();
        int rootSegment = slh.getStartSegment();


        for (int seg = startSegment, i = 0; i < getNumSegments(); i++, seg++) {
            if (seg == getNumSegments())
                seg = 0;
            if (i > 0 && rootSegment == seg)
                //we wrapped around, no more segments to scan
                return null;

            slh.setCurrentSegment((short) seg);

            StoredListChainSegment<T> segment = _segments[seg];

            if (slh.isActiveSegment()) {
                if (segment.iterNext(slh))
                    return slh;
            } else {
                if (segment.establishIterScanPos(slh))
                    return slh;
            }
        }
        return null; //all empty

    }


    /**
     * get the number of segments in this SL
     */
    private int getNumSegments() {
        return _segments.length;
    }

    public boolean isMultiObjectCollection() {
        return true;
    }

    public boolean isIterator() {
        return false;
    }

    /* (non-Javadoc)
     * @see com.j_spaces.kernel.IStoredList#dump(java.util.logging.Logger, java.lang.String)
     */
    public void dump(Logger logger, String msg) {
        if (logger.isLoggable(Level.INFO)) {
            logger.info(msg);

            IStoredListIterator<T> slh = null;
            try {
                for (slh = establishListScan(false); slh != null; slh = next(slh)) {
                    T subject = slh.getSubject();
                    if (subject != null)
                        logger.info(subject.getClass().getName());
                }
            } finally {
                if (slh != null)
                    slh.release();
            }
        }

    }

    /**
     * this method is called  by outside scan that want to quit the scan and return the slholder to
     * the factory
     */
    public void freeSLHolder(IStoredListIterator<T> slh) {
        if (slh != null) {
            SegmentedListIterator<T> si = (SegmentedListIterator<T>) slh;
            slh.release();
        }
    }

    private static class SegmentedListIteratorFactory implements PoolFactory<SegmentedListIterator> {
        public SegmentedListIterator create() {
            return new SegmentedListIterator();
        }
    }

    static class SegmentedListIterator<T>
            extends AbstractResource
            implements IStoredListIterator<T> {
        /**
         * This class is used in order to return and request (during scan) information from the
         * list- the subject is returned in separate field in order to to need synchronized access
         * to the m_ObjectInfo field that may be changed by other threads
         */

        boolean _randomScan;
        private short _startSegment;  // first segment in the scan
        private short _currentSegment; // current segment in the scan

        //PER SEGMENT VARS
        int _scanLimit;   //per segment
        int _currSegmentScanCount;
        ConcurrentSLObjectInfo<T> _cur;
        ConcurrentSLObjectInfo<T> _curElement;
        boolean _headToTail;

        public SegmentedListIterator() {
        }

        @Override
        protected void clean() {
            setStartSegment((short) 0);
            setCurrentSegment((short) 0);
            _scanLimit = 0;
            _currSegmentScanCount = 0;
            _cur = null;
            _curElement = null;
            _headToTail = false;
            _randomScan = false;
        }

        void setCurrentSegment(short currentSegment) {
            this._currentSegment = currentSegment;
        }

        /**
         * @return Returns the currentSegment
         */
        short getCurrentSegment() {
            return _currentSegment;
        }

        boolean isActiveSegment() {
            return _cur != null;
        }

        void setStartSegment(short segment) {
            this._startSegment = segment;
        }

        /**
         * @return Returns the startSegment
         */
        short getStartSegment() {
            return _startSegment;
        }

        /**
         * @param subject The subject to set.
         */
        public void setSubject(T subject) {
            throw new RuntimeException("invalid usage");
        }

        /**
         * @return Returns the subject.
         */
        public T getSubject() {
            return _curElement != null ? _curElement.getSubject() : null;
        }

    }


    //call it when no other action
    public void monitor() {
        for (int i = 0; i < _segments.length; i++)
            _segments[i].monitor();
    }


    //+++++++ HASH ENTRY METHODS- unsupported for basic SL
    public int getHashCode(int id) {
        throw new RuntimeException(" unsupported");
    }

    public Object getKey(int id) {
        throw new RuntimeException(" unsupported");
    }

    public IStoredList<T> getValue(int id) {
        throw new RuntimeException(" unsupported");
    }

    public boolean isNativeHashEntry() {
        return false;
    }

}
