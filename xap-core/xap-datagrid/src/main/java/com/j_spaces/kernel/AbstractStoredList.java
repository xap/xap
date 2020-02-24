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

import com.gigaspaces.internal.utils.threadlocal.ThreadLocalPool;
import com.j_spaces.kernel.list.IterableStoredListIterator;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An abstract implementation of the IStoredList, provides common
 *
 * @author Guy Korland
 * @since 6.1
 */
public abstract class AbstractStoredList<T>
        implements IStoredList<T>, Iterable<T> {

    protected static final int SCAN_LIMIT = 100000;
    protected static final int NUMBER_OF_OCCUPIED_POS_QUOTA = 1000;
    protected static final int MINIMAL_NUMBER_TO_HANDLE_POSITIONS = 2;


    final private static ThreadLocalPool<StoredListIterator> _SLHolderPool =
            new ThreadLocalPool<StoredListIterator>(new StoredListIterator.StoredListIteratorFactory());

    protected ArrayList<ObjectInfo<T>> m_BasicOccupied;
    protected ArrayList<ArrayList<ObjectInfo<T>>> m_AllOccupiedPos;
    protected int m_Size;
    protected boolean m_Support_Random_Scans;
    protected int m_LastChunk;
    protected int m_LastPos;
    protected ObjectInfo<T> m_Tail;
    protected ObjectInfo<T> m_Head;

    protected AbstractStoredList(boolean Support_Random_Scans) {
        m_Support_Random_Scans = Support_Random_Scans;
    }

    /*
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
     * establish a scan position. if random scanning supported we select a random position
     */
    @Override
    public StoredListIterator<T> establishListScan(boolean random_scan) {
        return establishListScan(random_scan,false/*alternatingThread*/);
    }
    @Override
    public StoredListIterator<T> establishListScan(boolean random_scan,boolean alternatingThread) {
        if (isEmpty())
            return null;
        StoredListIterator<T> slh = null;
        if (!alternatingThread)
            slh = _SLHolderPool.get();
        else
            slh = new StoredListIterator(true);

        if (!iterate(random_scan, slh)) {
            if (!alternatingThread)
                slh.release();
            return null;
        }

        return slh;
    }

    /**
     * @param random_scan
     * @param slh
     */
    protected abstract boolean iterate(boolean random_scan,
                                       StoredListIterator<T> slh);


    /**
     * get the next element in scan order
     */
    public IStoredListIterator<T> next(IStoredListIterator<T> slh) {
        if (!nextPos((StoredListIterator<T>) slh)) {
            slh.release();
            return null;
        }
        return slh;

    }

    /**
     * @param slh
     */
    public abstract boolean nextPos(StoredListIterator<T> slh);

    /**
     * this method is called  by outside scan that want to quit the scan and return the slholder to
     * the factory
     */
    public void freeSLHolder(IStoredListIterator<T> slh) {
        if (slh != null)
            slh.release();
    }

    protected boolean removeByObject_impl(Object obj) {
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

    protected boolean contains_impl(T obj) {
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
    protected void remove_impl(ObjectInfo<T> oiToRemove) {
        if (oiToRemove.getPosInList() == -1)
            throw new RuntimeException(" Stored list- called remove but element already deleted !");

        if (m_Size == 0)
            throw new RuntimeException(" Stored list- called remove but list is empty !");
        oiToRemove.setSubject(null);

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
     * @return true if the list is invalid
     */
    protected abstract boolean isInvalid();

    /**
     * store an element
     */
    protected ObjectInfo<T> store_impl(T subject) {

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

    protected ObjectInfo<T> storeBeforeCeiling_impl(ObjectInfo<T> theCeiling,
                                                    T subject) {
        if (m_Support_Random_Scans)
            throw new RuntimeException("storeBeforeCeiling and Support_Random_Scans!");
        ObjectInfo<T> oi = new ObjectInfo<T>(subject);
        ObjectInfo<T> ceiling = theCeiling;

        if (ceiling == null) {
            if (m_Size == 0)
                return (ObjectInfo<T>) add(subject);
            //put me as head
            m_Head.setForwardRef(oi);
            oi.setBackwardsRef(m_Head);
            m_Head = oi;
        } else {
            if (ceiling.getPosInList() == -1)
                throw new RuntimeException("storeBeforeCeiling and ceiling deleted!");

            oi.setBackwardsRef(ceiling.getBackwardsRef());
            ceiling.setBackwardsRef(oi);
            oi.setForwardRef(ceiling);
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

    public Iterator<T> iterator(boolean randomScan) {
        return new IterableStoredListIterator<T>(this, randomScan);
    }

    public Iterator<T> iterator() {
        return iterator(false);
    }

    public boolean isIterator() {
        return false;
    }


    static class ObjectInfo<T> implements Serializable, IObjectInfo<T> {
        private static final long serialVersionUID = -4410731885256582304L;

        /**
         * This class is the information unit in the list
         */
        private ObjectInfo<T> _backwrd; // towards tail (fifo)
        private ObjectInfo<T> _fwrd;   // towards head ( lifo)
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
        protected ObjectInfo<T> getBackwardsRef() {
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
}
