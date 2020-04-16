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

package com.gigaspaces.internal.cluster.node.impl.directPersistency.embeddedSyncList;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The synchronizing direct-persistency  embedded phantoms handler
 *
 * @author yechielf
 * @since 11.0
 */
@com.gigaspaces.api.InternalApi
public class PhantomsHandler {

    private final ConcurrentMap<String, IEmbeddedSyncOpInfo> _phantoms;
    private final EmbeddedSyncHandler _embeddedHandler;
    private final long _currentGenerationId;
    private final AtomicInteger _numPhantoms; //optimistic

    public PhantomsHandler(EmbeddedSyncHandler handler) {
        _phantoms = new ConcurrentHashMap<String, IEmbeddedSyncOpInfo>();//contains the phantoms
        _embeddedHandler = handler;
        _currentGenerationId = _embeddedHandler.getMainSyncHandler().getCurrentGenerationId();
        _numPhantoms = new AtomicInteger();

    }


    //NOTE- oi should be locked
    public void add(IEmbeddedSyncOpInfo oi, String uid) {
        addToNumPhantoms();
        if (_phantoms.putIfAbsent(uid, oi) != null) {
            IEmbeddedSyncOpInfo cur = _phantoms.get(uid);
            if (getLogger().isErrorEnabled()) {
                getLogger().error("[" + _embeddedHandler.getMainSyncHandler().getSpaceEngine().getFullSpaceName() + "]" + " DirectPersistencySyncHandler phantomsHandler:add phantom aleady exist uid=" + uid + " adding=" + oi.toString() + " existing=" + cur.toString());
            }
            throw new RuntimeException("trying to add phantom but phantom already exists uid=" + uid);
        }

        if (getLogger().isDebugEnabled()) {
            getLogger().debug("[" + _embeddedHandler.getMainSyncHandler().getSpaceEngine().getFullSpaceName() + "]" + " DirectPersistencySyncHandler phantomsHandler:add phantom " + oi.getOriginalOpInfo());
        }
    }

    private Logger getLogger() {
        return _embeddedHandler.getMainSyncHandler().getLogger();
    }

    private void addToNumPhantoms() {
        _numPhantoms.incrementAndGet();
    }

    private void subFromNumPhantoms() {
        int res = _numPhantoms.decrementAndGet();
        if (res < 0)
            throw new RuntimeException("inconsistent state- negative # phantoms " + res);
    }

    private boolean isEmpty() {
        return _numPhantoms.get() == 0;
    }

    //NOTE- oi should be locked
    public void removePhantom(IEmbeddedSyncOpInfo oi, String uid) {
        removePhantom(oi, uid, false /*alreadyVerifiedMap*/);
    }

    private void removePhantom(IEmbeddedSyncOpInfo oi, String uid, boolean alreadyVerifiedMap) {
        IEmbeddedSyncOpInfo cur = !alreadyVerifiedMap ? _phantoms.get(uid) : oi;
        if (cur == oi) {//real phantom, remove it
            getLogger().debug("[" + _embeddedHandler.getMainSyncHandler().getSpaceEngine().getFullSpaceName() + "] DirectPersistencySyncHandler phantomsHandler removing phantom " + oi.getOriginalOpInfo());
            _embeddedHandler.getMainSyncHandler().getListHandler().getIoHandler().removePhantom(uid, oi.getOriginalOpInfo().getGenerationId() != _currentGenerationId /*check existance*/, oi.getOriginalOpInfo().getGenerationId(), oi.getOriginalOpInfo().getSequenceNumber());
        } else {
            if (getLogger().isErrorEnabled()) {
                getLogger().error("[" + _embeddedHandler.getMainSyncHandler().getSpaceEngine().getFullSpaceName() + "] DirectPersistencySyncHandler phantomsHandler:removePhantom not fitting uid=" + uid + " operated=" + oi.toString() + " existing=" + ((cur != null) ? cur.toString() : " null"));
            }
            throw new RuntimeException("[" + _embeddedHandler.getMainSyncHandler().getSpaceEngine().getFullSpaceName() + "] DirectPersistencySyncHandler phantomsHandler:removePhantom not fitting uid=" + uid + " operated=" + oi.toString() + " existing=" + ((cur != null) ? cur.toString() : " null"));
        }
        oi.resetPhantom(uid);
        _phantoms.remove(uid, oi);
        subFromNumPhantoms();
    }

    public void onSpaceOpRemovePhantomIfExists(String uid) {
        if (isEmpty())
            return;
        IEmbeddedSyncOpInfo cur = _phantoms.get(uid);
        if (cur == null)
            return;
        synchronized (cur) {
            if (cur.containsPhantom(uid)) {
                if (!cur.isPersistedToMainList()) {
                    //persist the  the synclist record
                    _embeddedHandler.getMainSyncHandler().getListHandler().insertToPersistentSyncListFromEmbeddedList(cur.getOriginalOpInfo(), cur.getOriginalOpInfo().getGenerationId() != _currentGenerationId);
                    cur.setPersistedToMainList();
                }
                removePhantom(cur, uid, true /*alreadyVerifiedMap*/);
            }
        }
    }


}
