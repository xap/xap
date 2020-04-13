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

import com.gigaspaces.internal.utils.threadlocal.AbstractResource;
import com.gigaspaces.internal.utils.threadlocal.PoolFactory;
import com.gigaspaces.internal.utils.threadlocal.ThreadLocalPool;
import com.j_spaces.kernel.IObjectInfo;
import com.j_spaces.kernel.IReusableResourcePool;
import com.j_spaces.kernel.IStoredList;
import com.j_spaces.kernel.IStoredListIterator;
import com.j_spaces.kernel.SystemProperties;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * segmented Read/Write Lock version of StoredList to be used with JDK 1.5+ NOTE : some of the apis
 * of IStoredList are not supported used just for shortest path selected fifo & order related apis
 * not supported
 *
 * @author Yechiel Fefer
 * @version 1.0
 * @since 6.1
 */
@Deprecated //Unused internally since 15.5
@com.gigaspaces.api.InternalApi
public class RwlSegmentedStoredList<T>
        implements IStoredList<T> {

    /* array of segments. only head is kept per segment*/
    final private Segment<T>[] _segments;
    final private AtomicInteger _size = new AtomicInteger(0);

    final private static ThreadLocalPool<SegmentedListIterator> _SLHolderPool =
            new ThreadLocalPool<SegmentedListIterator>(new SegmentedListIteratorFactory());

    //when true the SL was marked invalid for further inserts
    //note that volatile is not needed here
    private boolean _invalid;


    // counts the number of adds - used to spread the objects evenly between segments
    private int addCounter = 0;
    // counts the number of scans - used to spread the start between segments
    private int scanCounter = 0;

    public RwlSegmentedStoredList(IReusableResourcePool<ReadWriteLock> locksPool) {
        int numOfSegments = Integer.getInteger(SystemProperties.ENGINE_STORED_LIST_SEGMENTS, SystemProperties.ENGINE_STORED_LIST_SEGMENTS_DEFAULT);

        // if set to 0 - use the default
        if (numOfSegments == 0)
            numOfSegments = SystemProperties.ENGINE_STORED_LIST_SEGMENTS_DEFAULT;

        _segments = new Segment[numOfSegments];

        //create segments & locks
        for (int seg = 0; seg < numOfSegments; seg++) {
            ReadWriteLock rwl = null;
            if (locksPool == null) {
                rwl = new ReentrantReadWriteLock();
            } else {
                rwl = locksPool.getResource((System.identityHashCode(this) + seg) % locksPool.size());
            }

            _segments[seg] = new Segment<T>((short) seg, rwl);
        }

    }

    public int size() {
        return _size.get();
    }

    /**
     * Returns true if the list is empty
     */
    public boolean isEmpty() {
        return size() == 0;
    }

    /**
     * Goes over all the segments and finds the first element.
     */
    public IObjectInfo<T> getHead() {
        for (int seg = 0; seg < getNumSegments(); seg++) {
            // try to avoid locking
            Segment<T> segment = _segments[seg];


            segment.getLock().readLock().lock();
            try {
                if (segment.isEmpty())
                    continue;

                for (ObjectInfo<T> oi = segment.getHead().getForwardRef(); oi != null; oi = oi.getForwardRef()) {
                    if (!oi.isDeleted() && oi.getSubject() != null) {
                        return oi;
                    }
                }

            } finally {
                segment.getLock().readLock().unlock();
            }
        }
        return null; //all empty
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
        return false;
    }

    /**
     * establish a scan position. we select a random segment to start from
     */
    public IStoredListIterator<T> establishListScan(boolean random_scan) {
        if (!random_scan)
            throw new RuntimeException("RwlSegmentedStoredList:: establishListScan non-random scans not supported");

        SegmentedListIterator<T> slh = _SLHolderPool.get();

        SegmentedListIterator<T> res = establishPos(slh);

        if (res == null)
            slh.release();

        return res;
    }

    /**
     * get the number of segments in this SL
     */
    private int getNumSegments() {
        return _segments.length;
    }

    /**
     * establish a scan position- select a segment
     */
    private SegmentedListIterator<T> establishPos(SegmentedListIterator<T> res) {
        int startSegment = drawSegmentNumber(false /*add*/);
        res.setStartSegment((short) startSegment);

        for (int seg = startSegment, i = 0; i < getNumSegments(); i++, seg++) {
            if (seg == getNumSegments())
                seg = 0;
            res.setCurrentSegment((short) seg);
            Segment<T> segment = _segments[seg];


            segment.getLock().readLock().lock();
            try {
                if (segment.isEmpty())
                    continue;

                for (ObjectInfo<T> oi = segment.getHead().getForwardRef(); oi != null; oi = oi.getForwardRef()) {
                    if (!oi.isDeleted() && oi.getSubject() != null) {
                        res.setCurrentOI(oi);
                        res.setSubject(oi.getSubject());

                        return res;
                    }
                }
            } finally {
                segment.getLock().readLock().unlock();
            }
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

            Segment<T> segment = _segments[seg];

            segment.getLock().readLock().lock();

            try {

                if (segment.isEmpty())
                    continue;

                for (ObjectInfo<T> oi = seg == startSegment ? slh.getCurrentOI()
                        .getForwardRef()
                        : segment.getHead().getForwardRef(); oi != null; oi = oi.getForwardRef()) {
                    if (!oi.isDeleted() && oi.getSubject() != null) {
                        slh.setCurrentOI(oi);
                        slh.setSubject(oi.getSubject());

                        return slh;
                    }
                }
            } finally {
                segment.getLock().readLock().unlock();
            }
        }
        return null; //all empty

    }

    /**
     * this method is called  by outside scan that want to quit the scan and return the slholder to
     * the factory
     */
    public void freeSLHolder(IStoredListIterator<T> slh) {
        if (slh != null)
            slh.release();
    }

    /**
     * store an element
     */
    public IObjectInfo<T> add(T subject) {
        return addImpl(subject, true /*lock*/);

    }

    public IObjectInfo<T> addUnlocked(T subject) {
        return addImpl(subject, false /*lock*/);

    }

    private IObjectInfo<T> addImpl(T subject, boolean lock) {
        //select a random segment to insert to
        int seg = drawSegmentNumber(true /*add*/);
        //int seg = System.identityHashCode(subject) %getNumSegments();
        Segment<T> segment = _segments[seg];


        ObjectInfo<T> oi = new ObjectInfo<T>(subject, (short) seg);

        if (lock)
            segment.getLock().writeLock().lock();
        try {
            return store_impl(segment, oi);
        } finally {
            if (lock)
                segment.getLock().writeLock().unlock();
        }

    }

    /**
     * store an element in segment
     */
    protected IObjectInfo<T> store_impl(Segment<T> segment, ObjectInfo<T> oi) {
        // if SL was marked as invalid, return null
        if (_invalid)
            return null;

        _size.incrementAndGet();

        segment.incrementSize();

        //connect to list. new object is replacing the current tail
        ObjectInfo<T> tail = segment.getTail();

        tail.setForwardRef(oi);
        oi.setBackwardRef(tail);
        segment.setTail(oi);

        return oi;
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
        ObjectInfo<T> oi = (ObjectInfo<T>) poi;
        int seg = oi.getSegment();
        Segment<T> segment = _segments[seg];
        if (lock)
            segment.getLock().writeLock().lock();
        try {

            remove_impl(segment, oi);
        } finally {
            if (lock)
                segment.getLock().writeLock().unlock();
        }

    }

    /**
     * remove an element described by ObjectInfo
     *
     * @param segment TODO
     */
    protected void remove_impl(Segment<T> segment, ObjectInfo<T> oi) {

        if (oi.isDeleted())
            throw new RuntimeException(" Stored list- called remove but element already deleted !");

        if (segment.getSize() == 0)
            throw new RuntimeException(" Stored list- called remove but list is empty !");
        oi.setSubject(null);

        if (oi.getForwardRef() != null)
            oi.getForwardRef().setBackwardRef(oi.getBackwardRef());

        if (oi.getBackwardRef() != null)
            oi.getBackwardRef().setForwardRef(oi.getForwardRef());

        if (segment.getTail() == oi)
            segment.setTail(oi.getBackwardRef());

        segment.decrementSize();
        _size.decrementAndGet();

        oi.setDeleted();

    }

    /**
     * given an object scan the list, find it and remove it, returns true if found
     */
    public boolean removeByObject(T obj) {
        throw new RuntimeException("RwlSegmentedStoredList::removeByObject not supported");
    }

    /**
     * is this object contained in the SL ?
     */
    public boolean contains(T obj) {
        throw new RuntimeException("RwlSegmentedStoredList::contains not supported");
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
        int lastSegmentLocked = -1;
        try {
            for (int i = 0; i < getNumSegments(); i++) {
                _segments[i].getLock().readLock().lock();
                lastSegmentLocked = i;
                if (_segments[i].getSize() > 0)
                    return false; //not successful

            }
            _invalid = true;
            return true;

        } finally {
            for (int i = lastSegmentLocked; i >= 0; i--) {
                _segments[i].getLock().readLock().unlock();

            }
        }
    }

    //draw a segment number for insertions/scans
    private int drawSegmentNumber(boolean add) {
        int tnum = (int) Thread.currentThread().getId();
        if (tnum % getNumSegments() == 0)
            tnum++;
        return add ? Math.abs(((tnum * addCounter++) % getNumSegments())) : Math.abs(((tnum * scanCounter++) % getNumSegments()));
    }

    /**
     * Defines a single segment of the list
     */
    final private static class Segment<T> extends ObjectInfo<T> {
        private static final long serialVersionUID = -5172326325174158826L;

        //head of list stored
        final private ObjectInfo<T> _head;
        //head of list stored
        private ObjectInfo<T> _tail;
        //size of segment
        volatile private int _size;
        //lock of segment
        final private ReadWriteLock _lock;

        Segment(short seg, ReadWriteLock lock) {
            super((T) null, seg);

            _lock = lock;

            // make the head and tail point to the segment
            // segment is a dummy ObjectInfo that saves head&tail null checks on add/remove
            _tail = _head = this;
        }

        private ObjectInfo<T> getHead() {
            return _head;
        }


        private ObjectInfo<T> getTail() {
            return _tail;
        }

        private void setTail(ObjectInfo<T> oi) {
            _tail = oi;
        }

        private int getSize() {
            return _size;
        }

        private int incrementSize() {
            return ++_size;
        }

        private int decrementSize() {
            return --_size;
        }

        private boolean isEmpty() {
            return _head.getForwardRef() == null;
        }

        private ReadWriteLock getLock() {
            return _lock;
        }

    }

    private static class ObjectInfo<T> implements Serializable, IObjectInfo<T> {
        private static final long serialVersionUID = -7542847245404958608L;
        /**
         * This class is the information unit in the list
         */
        private ObjectInfo<T> _forward; // towards tail (fifo)
        private ObjectInfo<T> _backward;   // towards head ( lifo)
        private short _segment; // segment in which it  resides
        private T _subject; // stored object
        private boolean _deleted;

        public ObjectInfo(T subject, short segment) {
            setSubject(subject);
            setSegment(segment);
        }

        /**
         * @param subject The subject to set.
         */
        public void setSubject(T subject) {
            _subject = subject;
        }

        /**
         * @return Returns the subject.
         */
        public T getSubject() {
            return _subject;
        }


        private void setForwardRef(ObjectInfo<T> forward) {
            this._forward = forward;
        }


        private ObjectInfo<T> getForwardRef() {
            return _forward;
        }


        private void setBackwardRef(ObjectInfo<T> backward) {
            _backward = backward;
        }


        private ObjectInfo<T> getBackwardRef() {
            return _backward;
        }

        /**
         * @param segment The segment to set.
         */
        private void setSegment(short segment) {
            this._segment = segment;
        }

        /**
         * @return Returns the segment.
         */
        private short getSegment() {
            return _segment;
        }

        private void setDeleted() {
            _deleted = true;
        }

        private boolean isDeleted() {
            return _deleted;
        }

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

    }

    private static class SegmentedListIteratorFactory implements PoolFactory<SegmentedListIterator> {
        public SegmentedListIterator create() {
            return new SegmentedListIterator();
        }
    }

    static final class SegmentedListIterator<T>
            extends AbstractResource
            implements IStoredListIterator<T> {
        /**
         * This class is used in order to return and request (during scan) information from the
         * list- the subject is returned in seperate field in order to to need synchronized access
         * to the m_ObjectInfo field that may be changed by other threads
         */

        private ObjectInfo<T> _currentOI;     // current object returned
        private T _subject;       // stored object
        private short _startSegment;  // first segment in the scan
        private short _currentSegment; // current segment in the scan
        private int _scanLimit;     //avoid perpetual scan situation

        public SegmentedListIterator() {
        }

        public SegmentedListIterator(short startSegment, ObjectInfo<T> oi,
                                     T obj, int slimit) {
            setCurrentOI(oi);
            setSubject(obj);
            setScanLimit(slimit);
            setStartSegment(startSegment);
            setCurrentSegment(startSegment);
        }

        @Override
        protected void clean() {
            setCurrentOI(null);
            setSubject(null);
            setScanLimit(0);
            setStartSegment((short) 0);
            setCurrentSegment((short) 0);
        }

        /**
         * @param currentOI The currentOI to set.
         */
        void setCurrentOI(ObjectInfo<T> currentOI) {
            this._currentOI = currentOI;
        }

        /**
         * @return Returns the currentOI.
         */
        ObjectInfo<T> getCurrentOI() {
            return _currentOI;
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
            this._subject = subject;
        }

        /**
         * @return Returns the subject.
         */
        public T getSubject() {
            return _subject;
        }

        /**
         * @param scanLimit The scanLimit to set.
         */
        private int setScanLimit(int scanLimit) {
            return this._scanLimit = scanLimit;
        }

        /**
         * @return Returns the scanLimit.
         */
        public int getScanLimit() {
            return _scanLimit;
        }
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
