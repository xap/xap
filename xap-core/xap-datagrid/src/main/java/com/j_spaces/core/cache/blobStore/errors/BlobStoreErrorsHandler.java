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
package com.j_spaces.core.cache.blobStore.errors;

import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.j_spaces.core.cache.CacheManager;
import com.j_spaces.core.cache.TypeData;
import com.j_spaces.core.cache.context.Context;
import com.j_spaces.core.cache.blobStore.BlobStoreRefEntryCacheInfo;
import com.j_spaces.core.cache.blobStore.sadapter.IBlobStoreStorageAdapter;

/**
 * handling entries after blob=store errors occurred
 *
 * @author yechiel
 * @since 11.0
 */
@com.gigaspaces.api.InternalApi
public class BlobStoreErrorsHandler {


    public static boolean revertOpOnBlobStoreError(CacheManager cm) {
        return ((IBlobStoreStorageAdapter) (cm.getStorageAdapter())).shouldRevertOpOnBlobStoreError();
    }

    public static void onFailedWrite(CacheManager cm, Context context, BlobStoreRefEntryCacheInfo eci, IEntryHolder eh) {
        try {
            if (!revertOpOnBlobStoreError(cm))
                return;  //currently we handle only ops in backup
            //call the entry in order to revert write op, reset dirty & pinned, remove from internal cache
            eh.setDeleted(true);
            eci.resetNonTransactionalFailedBlobstoreOpStatus(context,cm);
            cm.removeEntryFromCache(eh, false /*initiatedByEvictionStrategy*/, true /*locked*/, eci,
                    CacheManager.RecentDeleteCodes.NONE);

        } catch (Throwable ex) {
            cm.getLogger().error("error while reverting failed blob store op in backup,space " + cm.getEngine().getFullSpaceName() + " uid=" + eci.getUID() + " error=" + ex);
        }
    }

    public static void onFailedRemove(CacheManager cm, Context context, BlobStoreRefEntryCacheInfo eci, IEntryHolder eh) {
        try {
            if (!revertOpOnBlobStoreError(cm))
                return;  //currently we handle only ops in backup
            //call the entry in order to revert remove op, reset dirty & pinned, remove from internal cache
            eci.resetNonTransactionalFailedBlobstoreOpStatus(context,cm);
            eh.setDeleted(false);
        } catch (Throwable ex) {
            cm.getLogger().error("error while reverting failed blob store op in backup,space " + cm.getEngine().getFullSpaceName() + " uid=" + eci.getUID() + " error=" + ex);
        }
    }

    public static void onFailedUpdate(CacheManager cm, Context context, BlobStoreRefEntryCacheInfo eci, IEntryHolder eh) {
        /*
            1. before update we create copy of the BlobStoreEntryLayout (before the change) and store in context
            2. upon failure we do
              2.1 call eh in order to reset dirty & pinned, remove from internal cache
              2.1 remove entry from cache
              2.2 using layout we create entry holder and insert to cache
         */
        try {
            if (!revertOpOnBlobStoreError(cm))
                return;  //currently we handle only ops in backup
            //call the entry in order to reset dirty
            eci.resetNonTransactionalFailedBlobstoreOpStatus(context,cm);

            //remove index references
            TypeData typeData = cm.getTypeData(eh.getServerTypeDesc());
            cm.removeEntryReferences(eci, typeData, -1);
            //unregister from lease manager
            cm.getLeaseManager().unregister(eci, eh.getEntryData().getExpirationTime());

            eci.setBlobStoreVersion(context.getOriginalBlobStoreVersion());
            eh.updateEntryData(context.getOriginalData(), context.getOriginalData().getExpirationTime());
            eci.buildCrcForFields();

            //insert original refs
            cm.insertEntryReferences(context, eci, typeData, false/*applySequenceNumber*/);
            cm.getLeaseManager().registerEntryLease(eci, eh.getEntryData().getExpirationTime());

        } catch (Throwable ex) {
            cm.getLogger().error("error while reverting failed blob store op in backup,space " + cm.getEngine().getFullSpaceName() + " uid=" + eci.getUID() + " error=" + ex);
        }
    }

}
