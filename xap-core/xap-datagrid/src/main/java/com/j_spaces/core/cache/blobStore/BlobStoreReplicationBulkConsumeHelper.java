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

//
package com.j_spaces.core.cache.blobStore;

import com.gigaspaces.internal.server.space.SpaceEngine;
import com.j_spaces.core.cache.blobStore.storage.bulks.BlobStoreBulkInfo;
import com.j_spaces.core.cache.context.Context;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * when blob store is getting replication multiple ops assist by bulking in backup
 *
 * @author Boris
 * @since 11.0
 */

@com.gigaspaces.api.InternalApi
public class BlobStoreReplicationBulkConsumeHelper {
    private final SpaceEngine _engine;
    private final ConcurrentMap<Thread, Context> _contextMap;
    private volatile boolean _bulkPrepared = false;
    private long _lastProcessedKey;

    public BlobStoreReplicationBulkConsumeHelper(SpaceEngine engine) {
        _engine = engine;
        _contextMap = new ConcurrentHashMap<Thread, Context>();
    }

    public void prepareForBulking(boolean take) {
        if (!_engine.getCacheManager().useBlobStoreReplicationBackupBulks() || _bulkPrepared)
            return;
        Context context = _engine.getCacheManager().getCacheContext();
        context.setBlobStoreBulkInfo(new BlobStoreBulkInfo(_engine.getCacheManager(), take /*takeMultipleBulk*/));
        _contextMap.put(Thread.currentThread(), context);
        _bulkPrepared = true;
    }

    public Context getContext() {
        return _contextMap.get(Thread.currentThread());
    }

    public void flushBulk(long packetKey) {
        Context context = getContext();
        try {
            if (context == null) {
                // inject last processed key in case non bulk entry arrived
                _lastProcessedKey = packetKey;
                return;
            }
            if (context.isActiveBlobStoreBulk()) {
                context.getBlobStoreBulkInfo().bulk_flush(context, false/*only_if_chunk_reached*/, true);
                // inject last processed key when flush was successful
                _lastProcessedKey = packetKey;
                context.setBlobStoreBulkInfo(null);
            }
        } finally {
            _bulkPrepared = false;
            if (context != null) {
                _engine.getCacheManager().freeCacheContext(context);
                _contextMap.remove(Thread.currentThread());
            }
        }
    }

    public long getLastProcessedKey() {
        return _lastProcessedKey;
    }
}
