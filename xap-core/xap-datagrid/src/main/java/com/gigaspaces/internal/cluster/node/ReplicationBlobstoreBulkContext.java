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

package com.gigaspaces.internal.cluster.node;

import com.j_spaces.core.cache.blobStore.BlobStoreReplicationBulkConsumeHelper;

/**
 * @author Boris
 * @since 11.0.0 Contains context for incoming replication blobstore bulks
 */
@com.gigaspaces.api.InternalApi
public class ReplicationBlobstoreBulkContext {
    private int _blobStoreBulkId;
    boolean _flush;
    private BlobStoreReplicationBulkConsumeHelper _blobStoreReplicationBulkConsumeHelper;

    public ReplicationBlobstoreBulkContext(int bulkId) {
        _blobStoreBulkId = bulkId;
    }

    public int getBulkId() {
        return _blobStoreBulkId;
    }

    public boolean isPartOfBlobstoreBulk() {
        return _blobStoreBulkId != 0;
    }

    public boolean shouldFlush() {
        return _flush;
    }

    public void setBulkReplicationInfo(int bulkId) {
        if (bulkId != _blobStoreBulkId) {
            _blobStoreBulkId = bulkId;
            _flush = true;
        } else {
            _flush = false;
        }
    }

    public void nonBulkArrived() {
        _blobStoreBulkId = 0;
        _flush = true;
    }

    public BlobStoreReplicationBulkConsumeHelper getBlobStoreReplicationBulkConsumeHelper() {
        return _blobStoreReplicationBulkConsumeHelper;
    }

    public void setBlobStoreReplicationBulkConsumeHelper(BlobStoreReplicationBulkConsumeHelper blobStoreReplicationBulkConsumeHelper) {
        _blobStoreReplicationBulkConsumeHelper = blobStoreReplicationBulkConsumeHelper;
    }
}
