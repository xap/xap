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

import com.gigaspaces.internal.utils.concurrent.UncheckedAtomicReferenceFieldUpdater;
import com.j_spaces.kernel.IObjectInfo;
import com.j_spaces.kernel.list.ConcurrentSegmentedStoredList.SegmentedListIterator;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * TODO	add Javadoc
 *
 * @author Yechiel Fefer
 * @version 1.0
 * @since 7.1
 */
@com.gigaspaces.api.InternalApi
public class StoredListChainSegment<T> {
    /**
     * a nearly concurrent segmennt used in concurrent-segmented-sl implementation is nearly
     * concurrent ordered operations are not supported since there is only one insertion point
     */


    private final ConcurrentSLObjectInfo<T> _tail;  //insert point
    private final ConcurrentSLObjectInfo<T> _head;  //start scan from

    private int _iterCount;
    private final short _segment;

    private static final byte SUPPORT_FIFO = ((byte) 1) << 0;
    private static final byte PADDED = ((byte) 1) << 1;

    private final byte _flags;
    private final byte _healthCheck;

    static class ConcurrentSLObjectInfo<T> implements IObjectInfo<T> {
        private T _data;   // the stored object
        private volatile ConcurrentSLObjectInfo backwardRef;  //to tail
        private volatile ConcurrentSLObjectInfo forwardRef;  //to head

        private static final AtomicReferenceFieldUpdater<ConcurrentSLObjectInfo, ConcurrentSLObjectInfo> forwardUpdater = UncheckedAtomicReferenceFieldUpdater.newUpdater(ConcurrentSLObjectInfo.class, ConcurrentSLObjectInfo.class, "forwardRef");
        private static final AtomicReferenceFieldUpdater<ConcurrentSLObjectInfo, ConcurrentSLObjectInfo> backwardUpdater = UncheckedAtomicReferenceFieldUpdater.newUpdater(ConcurrentSLObjectInfo.class, ConcurrentSLObjectInfo.class, "backwardRef");

        private final short _segment;
        //info status values
        public final static byte inserting = 0;
        public final static byte inserted = 1;
        public final static byte removed = 2; //node logically removed
        public final static byte disconnected = 3; //node disconnected- must be removed first

        volatile byte _infoStatus = inserting;
        private final byte _healthCheck;


        public ConcurrentSLObjectInfo(T subject, short segment, ConcurrentSLObjectInfo backward, ConcurrentSLObjectInfo forward, byte healthCheck) {
            setSubject(subject);
            _segment = segment;
            if (backward != null) //save touching volatile
                backwardRef = backward;
            if (forward != null) //save touching volatile
                forwardRef = forward;
            _healthCheck = healthCheck;
        }

        /**
         * Sets the subject for this node.
         *
         * @param subject The subject to set.
         */
        public void setSubject(T subject) {
            _data = subject;
        }

        /**
         * @return Returns the subject at this node.
         */
        public T getSubject() {
            return _data;
        }

        public short getSegment() {
            return _segment;
        }

        boolean CAS_forward(ConcurrentSLObjectInfo expect, ConcurrentSLObjectInfo update) {
            return forwardUpdater.compareAndSet(this, expect, update);
        }

        boolean CAS_backward(ConcurrentSLObjectInfo expect, ConcurrentSLObjectInfo update) {
            return backwardUpdater.compareAndSet(this, expect, update);
        }

        void setBwd(ConcurrentSLObjectInfo update) {
            backwardRef = update;
        }

        void setFwd(ConcurrentSLObjectInfo update) {
            forwardRef = update;
        }

        ConcurrentSLObjectInfo getBwd() {
            return backwardRef;
        }

        ConcurrentSLObjectInfo getFwd() {
            return forwardRef;
        }


        void setNodeRemoved(boolean verify) {
            if (verify) {
                byte status = _infoStatus;
                if (status != inserted)
                    throw new RuntimeException("StoredListChainSegment:setNodeRemoved remove and node status =" + status);
            }
            _infoStatus = removed;

        }

        boolean isNodeRemoved() {
            byte status = _infoStatus;
            return status == removed || status == disconnected;
        }

        boolean isNodeInserted() {
            return _infoStatus == inserted;
        }

        void nodeInsertionEnded() {
            _infoStatus = inserted;

        }

        boolean isDisconnected() {
            return _infoStatus == disconnected;
        }

        void setDisconnected() {//should be called under lock
            _infoStatus = disconnected;
        }

        byte getHealthCheck() {
            return _healthCheck;
        }


    }
    //padded object info no 2 of them in same cache line
    static class ConcurrentSLObjectInfoPadded<T> extends ConcurrentSLObjectInfo<T> {
        long p1,p2,p3,p4,p5,p6,p7,p8;  //padding
        public ConcurrentSLObjectInfoPadded(T subject, short segment, ConcurrentSLObjectInfo backward, ConcurrentSLObjectInfo forward, byte healthCheck) {
            super(subject, segment, backward, forward,  healthCheck);
        }
    }


    StoredListChainSegment(short segment, boolean supportFifo,boolean padded) {
        _segment = segment;
        if (padded)
        {//padded means padding of head & tail
            _tail = new ConcurrentSLObjectInfoPadded(null, _segment, null, null, (byte) 0);
            _head = new ConcurrentSLObjectInfoPadded(null, _segment, _tail, null, (byte) 0);
        }
        else {
            _tail = new ConcurrentSLObjectInfo(null, _segment, null, null, (byte) 0);
            _head = new ConcurrentSLObjectInfo(null, _segment, _tail, null, (byte) 0);
        }
        _tail.setFwd(_head);
        _tail.nodeInsertionEnded();
        _head.nodeInsertionEnded();
        byte flags = 0;
        if (supportFifo)
            flags |= SUPPORT_FIFO;
        if (padded)
            flags |= PADDED;

        _flags = flags;
        _healthCheck = (byte) (System.identityHashCode(this));
    }

    public boolean isSupportFifo()
    {
        return (_flags & SUPPORT_FIFO) == SUPPORT_FIFO;

    }

    private ConcurrentSLObjectInfo insert(T entry) {
        ConcurrentSLObjectInfo newnode = new ConcurrentSLObjectInfo(entry, _segment, _tail, null, _healthCheck);
        insertNode(newnode);
        return newnode;
    }

    private void insertNode(ConcurrentSLObjectInfo newnode) {
        while (true) {//insertion loop
            ConcurrentSLObjectInfo cur_first = _tail.getFwd();
            newnode.setFwd(cur_first);
            if (_tail.CAS_forward(cur_first, newnode)) {
                //if fifo supported help out in "older" nodes ptr redirection
                if (isSupportFifo()) {
                    if (cur_first != _head && cur_first.getFwd().getBwd() == _tail) {
                        adjustInsertsPtrs(cur_first);
                    }
                }
                cur_first.setBwd(newnode);

                if (cur_first.isNodeRemoved()) {
                    //nodes that points at the head are not disconnected, maybe cur_first
                    //is removed so i may need to disconnect it
                    disconnect_node(cur_first);
                }
                newnode.nodeInsertionEnded();
                break;
            }
        }

    }


    private void adjustInsertsPtrs(final ConcurrentSLObjectInfo cur_first) {
        ConcurrentSLObjectInfo base = cur_first.getFwd();
        ArrayList<ConcurrentSLObjectInfo> nodes = null;

        while (true) {
            if (base == null || base.getBwd() != _tail)
                break;
            if (nodes == null) {
                nodes = new ArrayList<ConcurrentSLObjectInfo>();
                nodes.add(cur_first);
            }
            nodes.add(base);
            base = base.getFwd();
        }
        if (nodes == null || nodes.size() == 1)
            return; //no node to mend

        //go down
        for (int i = nodes.size() - 1; i >= 1; i--) {
            ConcurrentSLObjectInfo nodeToMend = nodes.get(i);
            ConcurrentSLObjectInfo toPointTo = nodes.get(i - 1);

            if (nodeToMend.getBwd() == _tail) {
                nodeToMend.CAS_backward(_tail, toPointTo);
            }
        }
    }


    private void removeNode(ConcurrentSLObjectInfo nodetodelete, boolean unlocked) {
        if (nodetodelete.getHealthCheck() != _healthCheck) {
            throw new IllegalStateException(
                    "Object was changed without explicitly calling the space api. " +
                            "XAP doesn't clone the objects for performance reasons, so if you want to change an object in the code that runs on the space, " +
                            "please make sure you clone the object before the change.");

        }
        //NOTE- a node that is pointing at the head is not removed just marked
        nodetodelete.setNodeRemoved(true /*verify*/);

        //do a pointer manipulation -effort to remove
        if (unlocked)
            disconnect_node_unlocked(nodetodelete);
        else
            disconnect_node(nodetodelete);

        return;
    }


    private boolean disconnect_node(ConcurrentSLObjectInfo nodetodrop) {
        //NOTE- a node that is pointing at the head is not removed just marked
        if (nodetodrop.isDisconnected() || nodetodrop.getBwd() == _tail)
            return true;

        synchronized (nodetodrop) {
            if (nodetodrop.isDisconnected() || nodetodrop.getBwd() == _tail)
                return true;
            //get my prev
            while (true) {
                ConcurrentSLObjectInfo prev = nodetodrop.getFwd();
                ConcurrentSLObjectInfo mybackward = nodetodrop.getBwd();

                synchronized (prev) {
                    if (prev != nodetodrop.getFwd())
                        continue;

                    prev.setBwd(mybackward);
                    mybackward.setFwd(prev);
                }
                nodetodrop.setDisconnected();
                return true;
            }

        }

    }

    private boolean disconnect_node_unlocked(ConcurrentSLObjectInfo nodetodrop) {
        //NOTE- a node that is pointing at the head is not removed just marked
        if (nodetodrop.isDisconnected() || nodetodrop.getBwd() == _tail)
            return true;

        //get my prev
        ConcurrentSLObjectInfo prev = nodetodrop.getFwd();
        ConcurrentSLObjectInfo mybackward = nodetodrop.getBwd();

        prev.setBwd(mybackward);
        mybackward.setFwd(prev);
        nodetodrop.setDisconnected();
        return true;
    }


    /**
     * Returns the first element in the list (fifo)
     */
    public IObjectInfo<T> getHead() {
        ConcurrentSLObjectInfo node = _head.getBwd();
        if (node == _tail || !node.isNodeInserted())
            return null;
        return node;
    }

    /**
     * Returns the value of the first element in the list (fifo)
     *
     * @return the value of the tail of the list
     */
    public T getObjectFromHead() {
        IObjectInfo<T> node = getHead();
        return node != null ? node.getSubject() : null;
    }


    /**
     * Remove an element described by ObjectInfo.
     *
     * [Head] - .. - [beforeOi] - [oi] - [afterOi] - .. - [Tail]
     *
     * @param oi an existing element between Head and Tail
     */
    public void remove(IObjectInfo<T> oi) {
        removeNode((ConcurrentSLObjectInfo) oi, false /*unlocked*/);
        oi.setSubject(null); //nullify subject
    }


    /**
     * Remove an element described by ObjectInfo, while the SL is unlocked.
     *
     * [Head] - .. - [beforeOi] - [oi] - [afterOi] - .. - [Tail]
     *
     * @param oi an existing element between Head and Tail
     */
    public void removeUnlocked(IObjectInfo<T> oi) {
        removeNode((ConcurrentSLObjectInfo) oi, true /*unlocked*/);
    }


    /**
     * given an object scan the list, find it and remove it, returns true if found
     */
    public boolean removeByObject(T obj) {
        return removeByObject_impl(obj);
    }


    private boolean removeByObject_impl(T obj) {
        for (ConcurrentSLObjectInfo<T> baseb = _tail.getFwd(); baseb != null; baseb = baseb.getFwd()) {
            if (baseb == _head)
                break;
            if (baseb.isNodeRemoved())
                continue;

            T other = baseb.getSubject();
            if (other != null && other.equals(obj)) {
                remove(baseb);
                return true;
            }
        }
        return false;
    }


    /**
     * store an element
     */
    public IObjectInfo<T> add(T subject) {
        return insert(subject);
    }


    //call it when no other action
    public void monitor() {
        int itersf = 0;
        int itersb = 0;
        int nonbalancedf = 0;
        int nonbalancedb = 0;
        int deletedf = 0;
        int deletedb = 0;
        int deletedInsidef = 0;
        int deletedInsideb = 0;

        for (ConcurrentSLObjectInfo basef = _head; basef != null; basef = basef.getBwd()) {
            itersf++;
            if (basef.isNodeRemoved()) {
                deletedf++;
                continue;
            }
            if (basef != _head) {
                ConcurrentSLObjectInfo myforward = basef.forwardRef;
                if (myforward.isNodeRemoved() && basef != _tail) {
                    deletedInsidef++;
                    continue;
                }
                if (myforward.backwardRef != basef) {
                    nonbalancedf++;
                    continue;
                }
            }
            if (basef != _tail) {
                ConcurrentSLObjectInfo myf = basef.backwardRef;
                if (myf.isNodeRemoved() && myf.getBwd() != _tail) {
                    deletedInsidef++;
                    continue;
                }
                if (myf.forwardRef != basef) {
                    nonbalancedf++;
                    continue;
                }
            }
        }
        for (ConcurrentSLObjectInfo baseb = _tail; baseb != null; baseb = baseb.getFwd()) {
            itersb++;
            if (baseb.isNodeRemoved()) {
                deletedb++;
                continue;
            }
            if (baseb != _head) {
                ConcurrentSLObjectInfo myforward = baseb.forwardRef;
                if (myforward.isNodeRemoved() && baseb != _tail) {
                    deletedInsideb++;
                    continue;
                }
                if (myforward.backwardRef != baseb) {
                    nonbalancedb++;
                    continue;
                }
            }
            if (baseb != _tail) {
                ConcurrentSLObjectInfo myf = baseb.backwardRef;
                if (myf.isNodeRemoved() && myf.getBwd() != _tail) {
                    deletedInsidef++;
                    continue;
                }
                if (myf.forwardRef != baseb) {
                    nonbalancedb++;
                    continue;
                }
            }
        }

        System.out.println("Seg= " + _segment + " itersf=" + itersf + " itersb=" + itersb +
                " nonbalancedf=" + nonbalancedf + " nonbalancedb=" + nonbalancedb +
                " deletedf=" + deletedf + " deletedb=" + deletedb +
                " deletedInsidef=" + deletedInsidef + " deletedInsideb=" + deletedInsideb);
    }

    /* get first element in scan for iterator established by the main list*/
    public boolean establishIterScanPos(SegmentedListIterator<T> iter) {
        boolean scanDown = isSupportFifo();//we always scan up from tail to head
        if (isSupportFifo() && iter._randomScan)
            scanDown = (++_iterCount) % 2 != 0;
        try {
            iter._cur = null;
            iter._curElement = null;
            iter._currSegmentScanCount = iter._scanLimit;
            iter._headToTail = scanDown;
            next(iter);
            return iter._curElement != null;
        } finally {
            if (iter._curElement == null)
                iter._cur = null;
        }

    }

    /* get next in scan for iterator established by the main list*/
    public boolean iterNext(SegmentedListIterator<T> iter) {
        try {
            if (iter._cur == null) {
                return false;
            }
            next(iter);
            return iter._curElement != null;
        } finally {
            if (iter._curElement == null)
                iter._cur = null;
        }

    }


    /**
     * get the next element in scan order
     */
    private void next(SegmentedListIterator<T> iter) {
        iter._curElement = null;
        if (iter._currSegmentScanCount < 0)
            return;
        if (iter._headToTail)
            next_down(iter);
        else
            next_up(iter);
    }

    private void next_down(SegmentedListIterator<T> iter) {
        if (iter._cur == _tail)
            return;
        if (iter._cur == null)
            iter._cur = _head;
        while (true) {
            iter._cur = iter._cur.getBwd();
            if (iter._cur == _tail)
                return;
            iter._currSegmentScanCount--;
            if (iter._cur.isNodeInserted()) {
                iter._curElement = iter._cur;
                return;
            }
            if (iter._currSegmentScanCount < 0)
                return;
        }
    }

    private void next_up(SegmentedListIterator<T> iter) {
        if (iter._cur == _head)
            return;
        if (iter._cur == null)
            iter._cur = _tail;
        while (true) {
            iter._cur = iter._cur.getFwd();
            if (iter._cur == _head)
                return;
            iter._currSegmentScanCount--;
            if (iter._cur.isNodeInserted()) {
                iter._curElement = iter._cur;
                return;
            }
            if (iter._currSegmentScanCount < 0)
                return;
        }
    }


}


