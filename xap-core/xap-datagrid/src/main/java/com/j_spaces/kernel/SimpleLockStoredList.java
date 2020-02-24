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

/*
 * Title:		RwlStoredList.java
 * Description:	Read/Write Lock version of StoredList
 * Company:		GigaSpaces Technologies
 * 
 * @author      Yehiel Fefer
 * @author		Moran Avigdor
 * @version		1.0 21/06/2005
 * @since		4.1 Build#1191
 */
package com.j_spaces.kernel;

import com.gigaspaces.internal.utils.concurrent.ReentrantSimpleLock;

import java.util.ArrayList;
import java.util.concurrent.locks.ReadWriteLock;


/**
 * Read/Write Lock version of StoredList to be used with JDK 1.5+
 */
@com.gigaspaces.api.InternalApi
public class SimpleLockStoredList<T> extends AbstractStoredList<T> {
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

    // Read/Write lock for read-mostly scenarios
    protected final ReentrantSimpleLock lock;

    final private static Scatter _scatter = new Scatter();


    public SimpleLockStoredList(boolean Support_Random_Scans) {
        this(Support_Random_Scans, (IReusableResourcePool<ReadWriteLock>) null);

    }

    /**
     * Create new stored list that will use the given ReadWriteLock for synchronization. This allows
     * lock reuse between stored lists
     */
    public SimpleLockStoredList(boolean Support_Random_Scans, IReusableResourcePool<ReadWriteLock> locksPool) {
        super(Support_Random_Scans);

        lock = new ReentrantSimpleLock();
    }

    public int size() {
        lock.lock();
        try {
            return m_Size;
        } finally {
            lock.unlock();
        }
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
        lock.lock();
        try {
            return m_Size == 0;
        } finally {
            lock.unlock();
        }
    }


    /**
     * get head of SL
     */
    public IObjectInfo<T> getHead() {
        lock.lock();
        try {
            return m_Head;
        } finally {
            lock.unlock();
        }
    }

    /**
     * get object from head  of SL
     */
    public T getObjectFromHead() {
        lock.lock();
        try {
            return m_Head != null ? m_Head.getSubject() : null;
        } finally {
            lock.unlock();
        }
    }

    protected boolean iterate(boolean random_scan, StoredListIterator<T> res) {
        lock.lock();
        try {
            if (m_Size <= 0)
                return false;

            random_scan = m_Support_Random_Scans && random_scan && (m_BasicOccupied != null);

            int scanLimit = res.isAlternatingThread()? Integer.MAX_VALUE : Math.max(m_Size, SCAN_LIMIT);

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
                    return false;

                res.setPivotOI(oi);
                res.setCurrentOI(oi);
                res.setSubject(oi.getSubject());
                res.setBackwardsScan(true);
                res.setScanLimit(scanLimit);

                return true;
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

                return true;
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

                return true;
            }
            return false;
        } finally {
            lock.unlock();
        }
    }


    public boolean nextPos(StoredListIterator<T> slh) {
        lock.lock();
        try {
            if (m_Size == 0)
                return false;

            if (slh.setScanLimit(slh.getScanLimit() - 1) <= 0)
                return false; // scan limit reached

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
                    return true;
                } else {
                    if (!slh.backwardsScan())
                        return false;
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
                return true;
            }

            return false;
        } finally {
            lock.unlock();
        }

    }


    /**
     * store an element
     */
    public IObjectInfo<T> add(T subject) {
        lock.lock();
        try {

            return store_impl(subject);
        } finally {
            lock.unlock();
        }
    }

    /**
     * store an element, while the SL is unlocked
     */
    public IObjectInfo<T> addUnlocked(T subject) {
        return store_impl(subject);
    }


    /**
     * remove an element described by ObjectInfo
     */
    public void remove(IObjectInfo<T> oi) {
        lock.lock();
        try {
            remove_impl((ObjectInfo<T>) oi);
        } finally {
            lock.unlock();
        }

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
        lock.lock();
        try {
            return removeByObject_impl(obj);
        } finally {
            lock.unlock();
        }

    }

    /**
     * is this object contained in the SL ?
     */
    public boolean contains(T obj) {
        lock.lock();
        try {
            return contains_impl(obj);
        } finally {
            lock.unlock();
        }
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

        try {
            lock.lock();

            if (m_Size <= 0) {
                m_Size = -1;
                return true;
            }

            return false;
        } finally {
            lock.unlock();
        }
    }

    /**
     * @return true if the list is invalid
     */
    protected boolean isInvalid() {
        return m_Size < 0;
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
