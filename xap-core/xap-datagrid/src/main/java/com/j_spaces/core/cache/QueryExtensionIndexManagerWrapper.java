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

package com.j_spaces.core.cache;

import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.query.extension.QueryExtensionEntryIterator;
import com.gigaspaces.query.extension.QueryExtensionManager;
import com.gigaspaces.query.extension.QueryExtensionProvider;
import com.gigaspaces.query.extension.QueryExtensionRuntimeInfo;
import com.gigaspaces.server.SpaceServerEntry;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@com.gigaspaces.api.InternalApi
public class QueryExtensionIndexManagerWrapper {
    private final QueryExtensionManager manager;
    private final ConcurrentMap<String, SpaceServerEntryImpl> indexedEntries = new ConcurrentHashMap<String, SpaceServerEntryImpl>();

    public QueryExtensionIndexManagerWrapper(QueryExtensionProvider provider, QueryExtensionRuntimeInfo info) {
        this.manager = provider.createManager(info);
    }

    public void close() throws IOException {
        this.manager.close();
    }

    public void introduceType(SpaceTypeDescriptor typeDescriptor) {
        manager.registerType(typeDescriptor);
    }

    public void insertEntry(SpaceServerEntryImpl entry, boolean fromTransactionalUpdate) {
        boolean inserted = manager.insertEntry(entry, false);
        if (inserted) {
            if (!fromTransactionalUpdate) {
                SpaceServerEntryImpl exist = indexedEntries.putIfAbsent(entry.getUid(), entry); // one existing in the map
                if (exist != null) //a lingering remove- wait for it
                    exist.waitForRemovalFromForeignIndex();
                indexedEntries.put(entry.getUid(), entry);
            }
        }
    }

    public void replaceEntry(SpaceServerEntryImpl entry) {
        boolean hasPreviousEntry = indexedEntries.containsKey(entry.getUid());
        boolean inserted = manager.insertEntry(entry, hasPreviousEntry);
        if (inserted || hasPreviousEntry) {
            //old is not null and new is null -> delete from map
            if (hasPreviousEntry && !inserted) {
                indexedEntries.remove(entry.getUid());
            } else {
                indexedEntries.put(entry.getUid(), entry);
            }
        }
    }

    public void removeEntry(SpaceServerEntry entry, QueryExtensionIndexRemoveMode queryExtensionIndexRemoveMode, int removedVersion) {
        SpaceServerEntryImpl exist = indexedEntries.get(entry.getUid());
        if (exist != null) {
            if (queryExtensionIndexRemoveMode == QueryExtensionIndexRemoveMode.NO_XTN && !exist.equals(entry))
                throw new RuntimeException("invalid ForeignIndexableServerEntry in remove uid=" + entry.getUid());
            manager.removeEntry(entry.getSpaceTypeDescriptor(), entry.getUid(), removedVersion);

            if (queryExtensionIndexRemoveMode == QueryExtensionIndexRemoveMode.NO_XTN) {
                indexedEntries.remove(entry.getUid(), entry);
                exist.setRemovedFromForeignIndex();
            } else {
//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
//TBD replate entry data in record !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            }
        }
    }

    public boolean filter(String typeName, String path, String operation, Object actual, Object matchedAgainst) {
        return manager.accept(typeName, path, operation, actual, matchedAgainst);
    }

    public QueryExtensionIndexEntryIteratorWrapper scanIndex(String typeName, String path, String operation, Object subject) {
        final QueryExtensionEntryIterator iterator = manager.queryByIndex(typeName, path, operation, subject);
        return new QueryExtensionIndexEntryIteratorWrapper(this, iterator);
    }

    public SpaceServerEntryImpl getByUid(String uid) {
        return indexedEntries.get(uid);
    }
}
