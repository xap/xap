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

package com.j_spaces.core.cache.blobStore.storage.bulks.delayedReplication;

import com.gigaspaces.internal.cluster.node.impl.directPersistency.IDirectPersistencyOpInfo;
import com.gigaspaces.internal.server.storage.IEntryHolder;

/**
 * used in blobstore bulks + embedded list in order to perform replication after flush
 *
 * @author yechielf
 * @since 11.0
 */
public abstract class DelayedReplicationBasicInfo {


    private final IEntryHolder _eh;
    private final DelayedReplicationOpCodes _opCode;
    private IDirectPersistencyOpInfo _directPersistencyOpInfo;

    public DelayedReplicationBasicInfo(IEntryHolder eh, DelayedReplicationOpCodes opCode) {
        _eh = eh;
        _opCode = opCode;
    }

    public IEntryHolder getEntry() {
        return _eh;
    }

    public DelayedReplicationOpCodes getOpCode() {
        return _opCode;
    }

    public IDirectPersistencyOpInfo getDirectPersistencyOpInfo() {
        return _directPersistencyOpInfo;
    }

    public void setDirectPersistencyOpInfo(IDirectPersistencyOpInfo directPersistencyOpInfo) {
        _directPersistencyOpInfo = directPersistencyOpInfo;
    }
}
