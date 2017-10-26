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
package com.j_spaces.core.cache.blobStore.optimizations;

import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.internal.server.space.LocalViewRegistrations;
import com.gigaspaces.internal.server.space.SpaceEngine;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.gigaspaces.internal.transport.AbstractProjectionTemplate;
import com.j_spaces.core.cache.IEntryCacheInfo;
import com.j_spaces.core.cache.context.Context;

/**
 * @author Yael Nahon
 * @since 12.1 .
 */
public class BlobStoreOperationOptimizations {
    public static boolean isConsiderOptimizedForBlobstore(SpaceEngine spaceEngine, Context context, ITemplateHolder template, IEntryCacheInfo pEntry) {
        if (!pEntry.isBlobStoreEntry() || template.getXidOriginatedTransaction() != null) {
            return false;
        }
        if (context.getOptimizedBlobStoreReadEnabled() != null) {
            return context.getOptimizedBlobStoreReadEnabled();
        }
        IServerTypeDesc serverTypeDesc = pEntry.getServerTypeDesc();
        boolean res = false;
        if (template.isReadOperation()) {
            res = isConsiderOptimizedReadForBlobstore(spaceEngine, template, serverTypeDesc, pEntry.getUID());
        } else if (template.getBatchOperationContext() != null && template.getBatchOperationContext().isClear()) {
            res = isConsiderOptimizedClearForBlobstore(spaceEngine, template, serverTypeDesc);
        } else if (template.isTakeOperation()) {
            res = isConsiderOptimizedTakeForBlobstore(spaceEngine, context, serverTypeDesc);
        }
        context.setOptimizedBlobStoreReadEnabled(res);
        return res;
    }

    private static boolean isConsiderOptimizedTakeForBlobstore(SpaceEngine spaceEngine, Context context, IServerTypeDesc serverTypeDesc) {
        boolean onBackup = context.isFromReplication() && spaceEngine.getSpaceImpl().isBackup();
        if (onBackup) {
            LocalViewRegistrations registrations = spaceEngine.getLocalViewRegistrations();
            return spaceEngine.getCacheManager().getTemplatesManager().isBlobStoreClearTakeOptimizationAllowedNotify(serverTypeDesc, onBackup)
                    && registrations != null
                    && registrations.isBlobStoreClearOptimizationAllowed(serverTypeDesc);
        }

        return false;
    }

    private static boolean isConsiderOptimizedClearForBlobstore(SpaceEngine spaceEngine, ITemplateHolder template, IServerTypeDesc serverTypeDesc) {
        //NOTE- clear is only on primary- on backup its take-by-id from replication
        if (!spaceEngine.getCacheManager().optimizedBlobStoreClear()) {
            return false;
        }
        return template.isOptimizedForBlobStoreOp(spaceEngine.getCacheManager())
                && (spaceEngine.getLocalViewRegistrations() == null || spaceEngine.getLocalViewRegistrations().isBlobStoreClearOptimizationAllowed(serverTypeDesc))
                && spaceEngine.getCacheManager().getTemplatesManager().isBlobStoreClearTakeOptimizationAllowedNotify(serverTypeDesc, false /*onBackup*/);
    }

    private static boolean isConsiderOptimizedReadForBlobstore(SpaceEngine spaceEngine, ITemplateHolder template, IServerTypeDesc serverTypeDesc, String entryUid) {
        AbstractProjectionTemplate projectionTemplate = template.getProjectionTemplate();
        if (!template.isOptimizedForBlobStoreOp(spaceEngine.getCacheManager())) {
            return false;
        }
        if (template.isReturnOnlyUid()) {
            return true;
        }
        if (projectionTemplate == null) {
            return false;
        }
        if (projectionTemplate.isMultiUidsProjection()) {
            return projectionTemplate.isAllIndexesProjections(serverTypeDesc, template, entryUid);
        }
        return projectionTemplate.isAllIndexesProjections(serverTypeDesc, template);
    }
}
