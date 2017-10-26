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

package com.gigaspaces.internal.server.storage;

import com.j_spaces.core.XtnEntry;

/**
 * {@link IEntryHolder} implementation which holds the entry's previous version in space before
 * update operation. <p> Relevant only for transactional operations where version can be increased
 * by more than +1 due to several update operations. <br> Eventually the previous version is
 * verified against replication's target current version. </p>
 *
 * @author idan
 * @since 8.0.4
 */
@com.gigaspaces.api.InternalApi
public class ReplicationEntryHolder extends EntryHolder {

    private final ShadowEntryHolder _previousEntry;
    //original entry is used by BlobStoreStorageAdapter
    private final IEntryHolder _originalEntry;

    public ReplicationEntryHolder(IEntryHolder entryHolder, XtnEntry xtnEntry) {
        // Creates a clone of the provided entry holder without the transaction info (lightweight)
        super(entryHolder);
        // Set the previous version if its available
        if (entryHolder.hasShadow())
            _previousEntry = entryHolder.getShadow();
        else
            _previousEntry = null;

        super.setWriteLockOwnerAndOperation(xtnEntry, entryHolder.getWriteLockOperation());
        _originalEntry = entryHolder;
    }

    @Override
    public boolean hasShadow() {
        return _previousEntry != null;
    }

    @Override
    public ShadowEntryHolder getShadow() {
        return _previousEntry;
    }

    @Override
    public IEntryHolder getOriginalEntryHolder() {
        return _originalEntry;
    }
}
