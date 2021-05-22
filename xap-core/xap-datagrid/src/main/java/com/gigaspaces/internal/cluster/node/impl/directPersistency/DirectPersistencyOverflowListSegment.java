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

package com.gigaspaces.internal.cluster.node.impl.directPersistency;


import com.gigaspaces.internal.cluster.node.impl.directPersistency.admin.DirectPersistencySyncListAdmin;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Iterator;
import java.util.LinkedList;

/**
 * the list segment  where the ops list is overflowen to persistency
 *
 * @author yechielf
 * @since 10.2
 */
@com.gigaspaces.api.InternalApi
public class DirectPersistencyOverflowListSegment implements SmartExternalizable {
    private static final long serialVersionUID = 1L;
    public static final int OVERFLOW_ELEMENT_MAX_SIZE = 10000;

    private long _generationId;
    private long _seq;
    private long _minRedoKey; //not updated during deletions
    private LinkedList<Long> _elements;
    private transient boolean _persisted;
    private transient boolean _dirty;


    public DirectPersistencyOverflowListSegment(long generationId, long seq) {
        _generationId = generationId;
        _seq = seq;
        _elements = new LinkedList<Long>();
        _minRedoKey = Long.MAX_VALUE;

    }

    public DirectPersistencyOverflowListSegment() {
    }

    public boolean isEmpty() {
        return _elements.isEmpty();
    }

    public boolean isFull() {
        return _elements.size() >= OVERFLOW_ELEMENT_MAX_SIZE;
    }

    public void add(IDirectPersistencySyncHandler main, IDirectPersistencyOpInfo oi) {
        if (oi.getRedoKey() == -1)
            throw new UnsupportedOperationException("invalid element");
        _dirty = true;
        //update the IDirectPersistencyOpInfo on disk it contains redokey now
        main.getListHandler().getIoHandler().update(oi);

        _elements.addLast(oi.getSequenceNumber());
        if (oi.getRedoKey() < _minRedoKey)
            _minRedoKey = oi.getRedoKey();
    }

    void persist(IDirectPersistencySyncHandler main) {
        if (!_dirty)
            return;
        if (_persisted) {
            main.getListHandler().getIoHandler().update(this);
        } else {
            main.getListHandler().getIoHandler().insert(this);
            _persisted = true;
        }
        _dirty = false;
    }

    public void remove(IDirectPersistencySyncHandler main) {
        if (!_persisted)
            return;
        main.getListHandler().getIoHandler().remove(this);
        _dirty = true;
        _persisted = false;
    }


    public int getNumElements() {
        return _elements.size();
    }

    public int syncToRedologConfirmation(IDirectPersistencySyncHandler main, long confirmed) {
        int res = 0;
        if (confirmed < _minRedoKey)
            return res;
        Iterator<IDirectPersistencyOpInfo> iter = new OverflowElementIterator(main, _elements);
        while (iter.hasNext()) {
            IDirectPersistencyOpInfo e = iter.next();
            if (e.getRedoKey() <= confirmed) {
                iter.remove();
                res++;
            } else
                break;
        }

        return res;

    }


    public long getGenerationId() {
        return _generationId;
    }

    public long getSequenceNumber() {
        return _seq;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(_generationId);
        out.writeLong(_seq);
        out.writeLong(_minRedoKey);
        out.writeInt(_elements.size());
        Iterator<Long> iter = _elements.iterator();
        while (iter.hasNext()) {
            out.writeLong(iter.next());
        }

    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        _persisted = true;
        _generationId = in.readLong();
        _seq = in.readLong();

        //currently unused- get current platform version and original platform version
        PlatformLogicalVersion curversion = PlatformLogicalVersion.getLogicalVersion();
        PlatformLogicalVersion myversion = DirectPersistencySyncListAdmin.getVersions().get(_generationId);
        if (myversion == null)
            throw new RuntimeException("platform version not found for generationid=" + _generationId);

        _minRedoKey = in.readLong();
        int size = in.readInt();
        _elements = new LinkedList<Long>();
        for (int i = 0; i < size; i++) {
            _elements.add(in.readLong());
        }
    }


    public static class OverflowElementIterator implements Iterator<IDirectPersistencyOpInfo> {
        private final IDirectPersistencySyncHandler _main;
        private final Iterator<Long> _iter;
        private IDirectPersistencyOpInfo _cur;

        public OverflowElementIterator(IDirectPersistencySyncHandler main, LinkedList<Long> elements) {
            _main = main;
            _iter = elements.iterator();
        }

        @Override
        public IDirectPersistencyOpInfo next() {
            _cur = _main.getListHandler().getIoHandler().get(_main.getCurrentGenerationId(), _iter.next());
            return _cur;
        }

        @Override
        public boolean hasNext() {
            return _iter.hasNext();
        }

        @Override
        public void remove() {

            _main.getListHandler().getIoHandler().remove(_cur);
            _iter.remove();
        }

    }

}

