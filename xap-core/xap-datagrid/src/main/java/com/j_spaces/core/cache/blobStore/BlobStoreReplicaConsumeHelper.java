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
 * when blob store is recoverd via replication assist by bulking
 *
 * @author yechiel
 * @since 10.1
 */

@com.gigaspaces.api.InternalApi
public class BlobStoreReplicaConsumeHelper {
    private final SpaceEngine _engine;
    private final ConcurrentMap<Thread, Context> _contextMap;

    public BlobStoreReplicaConsumeHelper(SpaceEngine engine) {
        _engine = engine;
        _contextMap = new ConcurrentHashMap<Thread, Context>();
    }

    public void prepareForBulking() {
        if (!_engine.getCacheManager().useBlobStoreBulks())
            return;
        Context context = _engine.getCacheManager().getCacheContext();
        context.setBlobStoreBulkInfo(new BlobStoreBulkInfo(_engine.getCacheManager(), false /*takeMultipleBulk*/));

        _contextMap.put(Thread.currentThread(), context);
    }

    public Context getContext() {
        return _contextMap.get(Thread.currentThread());
    }

    public void flushBulk() {
        Context context = getContext();
        if (context == null)
            return;
        try {
            if (context.isActiveBlobStoreBulk()) {
                //if off-heap in case of bulk we first need to flush to the blob-store
                context.getBlobStoreBulkInfo().bulk_flush(context, false/*only_if_chunk_reached*/, true);
            }
        } finally {
            _engine.getCacheManager().freeCacheContext(context);
            _contextMap.remove(Thread.currentThread());
        }
    }
}
