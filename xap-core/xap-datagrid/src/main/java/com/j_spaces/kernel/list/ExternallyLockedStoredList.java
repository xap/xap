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
import com.j_spaces.kernel.IOrderedList;
import com.j_spaces.kernel.IStoredList;
import com.j_spaces.kernel.IStoredListIterator;

import java.io.Serializable;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * no Lock version of StoredList locks should be external to this class scanning after establishing
 * a starting position should be left lock free
 */
@com.gigaspaces.api.InternalApi
public class ExternallyLockedStoredList<T>
        implements IStoredList<T>, IOrderedList<T> {
    /**
     * Used to support pseudo random scan start.
     *
     * @author GuyK
     * @since 7.0.1
     */
    private static class Scatter {

        // Used by the
        static private int addCounter = 0;

        public int next(int size) {
            // random start
            int threadID = (int) Thread.currentThread().getId();
            return Math.abs((threadID * addCounter++) % size);
        }
    }

    private static final int SCAN_LIMIT = 100000;
    private static final int NUMBER_OF_OCCUPIED_POS_QUOTA = 1000;
    private static final int MINIMAL_NUMBER_TO_HANDLE_POSITIONS = 2;


    final private static ThreadLocalPool<StoredListIterator> _SLHolderPool =
            new ThreadLocalPool<StoredListIterator>(new StoredListIterator.StoredListIteratorFactory());

    private ArrayList<ObjectInfo<T>> m_BasicOccupied;
    private ArrayList<ArrayList<ObjectInfo<T>>> m_AllOccupiedPos;
    private volatile int m_Size;
    private boolean m_Support_Random_Scans;
    private int m_LastChunk;
    private int m_LastPos;
    private ObjectInfo<T> m_Tail;
    private ObjectInfo<T> m_Head;


    final private static Scatter _scatter = new Scatter();


    public ExternallyLockedStoredList(boolean Support_Random_Scans) {
        m_Support_Random_Scans = Support_Random_Scans;

    }


    public int size() {
        return m_Size;
    }

    /**
     * return true if we can save iterator creation and get a single entry
     *
     * @return true if we can optimize
     */
    public boolean optimizeScanForSingleObject() {
        return size() <= 1;
    }


    public boolean isEmpty() {
        return m_Size == 0;
    }


    /**
     * get head of SL
     */
    public IObjectInfo<T> getHead() {
        return m_Head;
    }

    /**
     * get object from head  of SL
     */
    public T getObjectFromHead() {
        return m_Head != null ? m_Head.getSubject() : null;
    }

    protected StoredListIterator<T> establishPos(boolean random_scan, StoredListIterator<T> res) {
        if (m_Size <= 0)
            return null;

        random_scan = m_Support_Random_Scans && random_scan && (m_BasicOccupied != null);

        int scanLimit = Math.max(m_Size, SCAN_LIMIT);

        ObjectInfo<T> oi = null;
        if (!random_scan || m_Size == 1) {
            // non random scan- start from head (oldest)
            oi = m_Head;
            while (oi != null) {
                if (oi.getSubject() != null)
                    break;
                oi = oi.getBackwardsRef();
            }
            if (oi == null)
                return null;

            res.setPivotOI(oi);
            res.setCurrentOI(oi);
            res.setSubject(oi.getSubject());
            res.setBackwardsScan(true);
            res.setScanLimit(scanLimit);

            return res;
        }

        int num = _scatter.next(m_Size);
        int pos_inside = num;
        ObjectInfo<T> ss = null;
        if (num >= NUMBER_OF_OCCUPIED_POS_QUOTA) {
            int chunknum = num / NUMBER_OF_OCCUPIED_POS_QUOTA;
            pos_inside = num % NUMBER_OF_OCCUPIED_POS_QUOTA;

            ArrayList<ObjectInfo<T>> al = m_AllOccupiedPos.get(chunknum);
            ss = al.get(pos_inside);
        } else {
            ss = m_BasicOccupied.get(pos_inside);
        }
        oi = ss;
        // scan fwd -
        while (oi != null) {
            if (oi.getSubject() != null)
                break;
            oi = oi.getForwardRef();
        }
        if (oi != null) {
            res.setPivotOI(oi);
            res.setCurrentOI(oi);
            res.setSubject(oi.getSubject());
            res.setTryForwardScan(true);
            res.setBackwardsScan(true);
            res.setScanLimit(scanLimit);

            return res;
        }

        // scan backwd -
        oi = ss.getBackwardsRef();
        while (oi != null) {
            if (oi.getSubject() != null)
                break;
            oi = oi.getBackwardsRef();
        }
        if (oi != null) {
            res.setPivotOI(oi);
            res.setCurrentOI(oi);
            res.setSubject(oi.getSubject());
            res.setTryForwardScan(false);
            res.setBackwardsScan(true);
            res.setScanLimit(scanLimit);

            return res;
        }
        return null;
    }

    protected StoredListIterator<T> establishOrderedPos(ObjectInfo<T> OrderedScanPivot, boolean ascending, StoredListIterator<T> res) {
        if (m_Size <= 0)
            return null;

        int scanLimit = Math.max(m_Size, SCAN_LIMIT);

        if (OrderedScanPivot == null) { //one-directional scan
            if (!ascending)
                return establishPos(false, res); //descending

            //ascending- from tail
            ObjectInfo<T> oi = m_Tail;
            while (oi != null) {
                if (oi.getSubject() != null)
                    break;
                oi = oi.getForwardRef();
            }
            if (oi == null)
                return null;

            res.setPivotOI(oi);
            res.setCurrentOI(oi);
            res.setSubject(oi.getSubject());
            res.setBackwardsScan(false);
            res.setTryForwardScan(true);
            res.setScanLimit(scanLimit);

            return res;

        }

        res.setPivotOI(OrderedScanPivot);
        res.setCurrentOI(OrderedScanPivot);
        res.setSubject(OrderedScanPivot.getSubject());
        res.setBackwardsScan(!ascending);
        res.setTryForwardScan(ascending);
        res.setScanLimit(scanLimit);

        return res;

    }

    /*
     * this method should stay unlocked-suize + ptrs of objectinfo are volatile
     */
    protected IStoredListIterator<T> nextPos(StoredListIterator<T> slh) {
        if (m_Size == 0)
            return null;

        if (slh.setScanLimit(slh.getScanLimit() - 1) <= 0)
            return null; // scan limit reached

        ObjectInfo<T> oi = null;
        if (slh.tryForwardScan()) {
            oi = slh.getCurrentOI().getForwardRef();
            // scan fwd - the pick can be deleted (subject is null)
            while (oi != null) {
                if (oi.getSubject() != null)
                    break;
                oi = oi.getForwardRef();
            }
            if (oi != null) {
                slh.setCurrentOI(oi);
                slh.setSubject(oi.getSubject());
                return slh;
            } else {
                if (!slh.backwardsScan())
                    return null;
                // change direction
                slh.setTryForwardScan(false);
                slh.setCurrentOI(slh.getPivotOI());
            }

        } //if (slh.m_FwdScan)
        // scan backwd - the pick can be deleted (subject is null)
        oi = slh.getCurrentOI().getBackwardsRef();
        while (oi != null) {
            if (oi.getSubject() != null)
                break;
            oi = oi.getBackwardsRef();
        }
        if (oi != null) {
            slh.setCurrentOI(oi);
            slh.setSubject(oi.getSubject());
            return slh;
        }

        return null;

    }


    /**
     * store an element
     */
    public IObjectInfo<T> add(T subject) {

        return store_impl(subject);
    }

    /**
     * store an element, while the SL is unlocked
     */
    public IObjectInfo<T> addUnlocked(T subject) {
        return store_impl(subject);
    }


    /**
     * given an ObjectInfo of an existing element, store a new element right "before" this one.
     * Method used in OrderedIndex object when the MasterList reflects the order of elements- head
     * is the largest, tail the smallest
     */
    public IObjectInfo<T> storeBeforeCeiling(IObjectInfo<T> theCeiling, T subject) {
        return storeBeforeCeiling_impl((ObjectInfo<T>) theCeiling, subject);
    }


    /**
     * in an ordered SL, get the element larger than ref
     */
    public IObjectInfo<T> getLargerInOrder(IObjectInfo<T> ref) {
        return ((ObjectInfo<T>) ref).getForwardRef();
    }

    /**
     * in an ordered SL, get the element smaller than ref
     */
    public IObjectInfo<T> getSmallerInOrder(IObjectInfo<T> ref) {
        return ((ObjectInfo<T>) ref).getBackwardsRef();
    }

    /**
     * remove an element described by ObjectInfo
     */
    public void remove(IObjectInfo<T> oi) {
        remove_impl((ObjectInfo<T>) oi);

    }

    /**
     * remove an element described by ObjectInfo, while the SL is unlocked
     */
    public void removeUnlocked(IObjectInfo<T> oi) {
        remove_impl((ObjectInfo<T>) oi);

    }


    /**
     * given an object scan the list, find it and remove it, returns true if found
     */
    public boolean removeByObject(T obj) {
        return removeByObject_impl(obj);

    }

    /**
     * is this object contained in the SL ?
     */
    public boolean contains(T obj) {
        return contains_impl(obj);
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
        if (size() > 0)
            return false;


        if (m_Size <= 0) {
            m_Size = -1;
            return true;
        }

        return false;
    }

    /**
     * @return true if the list is invalid
     */
    protected boolean isInvalid() {
        return m_Size < 0;
    }

    //YP++++++++++++++++
   /*
    * @see com.j_spaces.kernel.IStoredList#dump(java.util.logging.Logger, java.lang.String)
    */
    public void dump(Logger logger, String msg) {
        if (logger.isInfoEnabled()) {
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
     * establish a scan position. if random scanning supported we select a random position
     */
    public StoredListIterator<T> establishListScan(boolean random_scan) {
        if (isEmpty())
            return null;
        StoredListIterator<T> slh = _SLHolderPool.get();

        StoredListIterator<T> res = establishPos(random_scan, slh);

        if (res == null)
            slh.release();

        return res;
    }

    /**
     * establish a scan position in one direction ascending (twrds head) or descending (towards
     * tail). if random scanning supported starting at a predefined pos this API is used by the
     * OrderedIndex masterList to establish an index scan
     */
    public IStoredListIterator<T> establishListOrderedScan(
            IObjectInfo<T> OrderedScanPivot, boolean ascending) {
        if (m_Size <= 0)
            return null;
        StoredListIterator<T> slh = _SLHolderPool.get();

        StoredListIterator<T> res = establishOrderedPos((ObjectInfo<T>) OrderedScanPivot,
                ascending, slh);

        if (res == null)
            slh.release();

        return res;
    }

    /**
     * get the next element in scan order
     */
    public IStoredListIterator<T> next(IStoredListIterator<T> slh) {
        IStoredListIterator<T> slnext = nextPos((StoredListIterator<T>) slh);

        if (slnext == null)
            slh.release();

        return slnext;

    }


    /**
     * this method is called  by outside scan that want to quit the scan and return the slholder to
     * the factory
     */
    public void freeSLHolder(IStoredListIterator<T> slh) {
        if (slh != null)
            slh.release();
    }

    private boolean removeByObject_impl(Object obj) {
        ObjectInfo<T> oi = m_Tail;

        while (oi != null) {
            if (oi.getSubject() != null)
                if (oi.getSubject().equals(obj))
                    break;
            oi = oi.getForwardRef();
        }
        if (oi == null)
            return false;

        remove(oi);
        return true;

    }

    private boolean contains_impl(T obj) {
        ObjectInfo<T> oi = m_Tail;
        while (oi != null) {
            if (oi.getSubject() != null)
                if (oi.getSubject().equals(obj))
                    return true;
            oi = oi.getForwardRef();
        }
        return false;
    }

    /**
     * remove an element described by ObjectInfo
     */
    private void remove_impl(ObjectInfo<T> oiToRemove) {
        if (oiToRemove.getPosInList() == -1)
            throw new RuntimeException(" Stored list- called remove but element already deleted !");

        if (m_Size == 0)
            throw new RuntimeException(" Stored list- called remove but list is empty !");
        //oiToRemove.setSubject(null); dont set to null- scans are concurrent

        if (oiToRemove.getBackwardsRef() != null)
            oiToRemove.getBackwardsRef()
                    .setForwardRef(oiToRemove.getForwardRef());

        if (oiToRemove.getForwardRef() != null)
            oiToRemove.getForwardRef()
                    .setBackwardsRef(oiToRemove.getBackwardsRef());

        if (m_Tail == oiToRemove)
            m_Tail = oiToRemove.getForwardRef();
        if (m_Head == oiToRemove)
            m_Head = oiToRemove.getBackwardsRef();

        m_Size--;

        if (m_Support_Random_Scans && (m_BasicOccupied != null)) {
            ArrayList<ObjectInfo<T>> last_al = m_LastChunk > 0 ? m_AllOccupiedPos.get(m_LastChunk)
                    : m_BasicOccupied;

            int my_chunknum = (oiToRemove.getPosInList() - 1)
                    / NUMBER_OF_OCCUPIED_POS_QUOTA;
            int my_pos_inside = (oiToRemove.getPosInList() - 1)
                    % NUMBER_OF_OCCUPIED_POS_QUOTA;
            ArrayList<ObjectInfo<T>> my_al = my_chunknum > 0 ? m_AllOccupiedPos.get(my_chunknum)
                    : m_BasicOccupied;

            if (m_Size > 0
                    && (my_chunknum != m_LastChunk || my_pos_inside != m_LastPos)) { //swap positions
                ObjectInfo<T> soi = last_al.get(m_LastPos);
                soi.setPosInList(oiToRemove.getPosInList());
                my_al.set(my_pos_inside, soi);

            }
            last_al.remove(m_LastPos);
            if (m_LastPos == 0 && m_LastChunk > 0)
                m_AllOccupiedPos.remove(m_LastChunk);

            m_LastPos--;
            if (m_LastPos < 0) {
                if (m_LastChunk > 0) {
                    m_LastChunk--;
                    m_LastPos = NUMBER_OF_OCCUPIED_POS_QUOTA - 1;
                    if (m_LastChunk == 0)
                        m_AllOccupiedPos = null;
                } else
                    m_LastPos = 0;
            }
        }//if (m_Support_Random_Scans)

        oiToRemove.setPosInList(-1);

    }


    /**
     * store an element
     */
    private ObjectInfo<T> store_impl(T subject) {

        // if SL was marked as invalid, return null
        if (isInvalid())
            return null;

        ++m_Size;

        ObjectInfo<T> oi = null;
        if (m_Support_Random_Scans && (m_BasicOccupied == null)
                && m_Size >= MINIMAL_NUMBER_TO_HANDLE_POSITIONS) {
            // the list have reached a critical size- create positions arraylist
            m_BasicOccupied = new ArrayList<ObjectInfo<T>>();
            oi = m_Tail;
            int indx = 1;
            while (oi != null) {
                m_BasicOccupied.add(oi);
                m_LastPos = indx - 1;
                oi.setPosInList(indx++);
                oi = oi.getForwardRef();
            }
        }
        oi = new ObjectInfo<T>(subject);

        //connect to list. new object is replacing the current tail
        if (m_Tail != null) {
            oi.setForwardRef(m_Tail);
            m_Tail.setBackwardsRef(oi);
        }
        m_Tail = oi;
        //will it replace the head too ?
        if (m_Head == null)
            m_Head = oi;

        if (m_Support_Random_Scans && (m_BasicOccupied != null)) {
            ArrayList<ObjectInfo<T>> spos = null;

            m_LastPos = m_Size > 1 ? (m_LastPos + 1) : 0;
            if (m_LastPos >= NUMBER_OF_OCCUPIED_POS_QUOTA) {
                m_LastPos = 0;
                ++m_LastChunk;
                if (m_AllOccupiedPos == null) {
                    m_AllOccupiedPos = new ArrayList<ArrayList<ObjectInfo<T>>>();
                    m_AllOccupiedPos.add(m_BasicOccupied);
                }
                m_AllOccupiedPos.add(new ArrayList<ObjectInfo<T>>());

            }
            spos = m_LastChunk == 0 ? m_BasicOccupied
                    : m_AllOccupiedPos.get(m_LastChunk);
            spos.add(oi);
            oi.setPosInList(m_Size);
        }
        return oi;
    }

    private ObjectInfo<T> storeBeforeCeiling_impl(ObjectInfo<T> theCeiling,
                                                  T subject) {
        if (m_Support_Random_Scans)
            throw new RuntimeException("storeBeforeCeiling and Support_Random_Scans!");
        ObjectInfo<T> oi = new ObjectInfo<T>(subject);
        ObjectInfo<T> ceiling = theCeiling;

        if (ceiling == null) {
            if (m_Size == 0)
                return (ObjectInfo<T>) add(subject);
            //put me as head
            oi.setBackwardsRef(m_Head);
            m_Head.setForwardRef(oi);
            m_Head = oi;
        } else {
            if (ceiling.getPosInList() == -1)
                throw new RuntimeException("storeBeforeCeiling and ceiling deleted!");

            oi.setBackwardsRef(ceiling.getBackwardsRef());
            oi.setForwardRef(ceiling);
            ceiling.setBackwardsRef(oi);
            if (oi.getBackwardsRef() != null)
                oi.getBackwardsRef().setForwardRef(oi);

            if (m_Tail == ceiling)
                m_Tail = oi;

        }

        m_Size++;
        return oi;
    }


    public boolean isMultiObjectCollection() {
        return true;
    }

    public boolean isIterator() {
        return false;
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


//YP_______________________   

    protected static class ObjectInfo<T> implements Serializable, IObjectInfo<T> {
        private static final long serialVersionUID = -4884728672743402002L;
        /**
         * This class is the information unit in the list
         */
        private volatile ObjectInfo<T> _backwrd; // towards tail (fifo)
        private volatile ObjectInfo<T> _fwrd;   // towards head ( lifo)
        private int _pos;    // serial pos in list
        private T _subject; // stored object

        public ObjectInfo(T subject) {
            setSubject(subject);
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

        /**
         * @param backwrd The backwrd to set.
         */
        public void setBackwardsRef(ObjectInfo<T> backwrd) {
            this._backwrd = backwrd;
        }

        /**
         * @return Returns the backwrd.
         */
        public ObjectInfo<T> getBackwardsRef() {
            return _backwrd;
        }

        /**
         * @param fwrd The fwrd to set.
         */
        public void setForwardRef(ObjectInfo<T> fwrd) {
            _fwrd = fwrd;
        }

        /**
         * @return Returns the fwrd.
         */
        public ObjectInfo<T> getForwardRef() {
            return _fwrd;
        }

        /**
         * @param pos The pos to set.
         */
        public void setPosInList(int pos) {
            this._pos = pos;
        }

        /**
         * @return Returns the pos.
         */
        public int getPosInList() {
            return _pos;
        }

    }

    public static class StoredListIterator<T>
            extends AbstractResource
            implements IStoredListIterator<T> {

        public static class StoredListIteratorFactory implements PoolFactory<StoredListIterator> {
            public StoredListIterator create() {
                return new StoredListIterator();
            }
        }

        /**
         * This class is used in order to return and request (during scan) information from the
         * list- the subject is returned in separate field in order to to need synchronized access
         * to the m_ObjectInfo field that may be changed by other threads
         */

        private ObjectInfo<T> _currentOI; // current List basic unit
        private ObjectInfo<T> _pivotOI;   // scan pivot
        private T _subject;   // stored object
        private boolean _tryFwdScan; // tail to head (lifo),true = in fwd direction, valid in random scan
        private boolean _backward;  //true - from head to tail (fifo)
        private int _scanLimit; //avoid perpetual scan situation


        public StoredListIterator() {
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

    }

}
