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

import com.gigaspaces.internal.server.storage.IEntryData;
import com.j_spaces.core.SpaceOperations;
import com.j_spaces.core.cache.context.Context;

/**
 * error info used to revert failed bulk flush- currently used in backup
 *
 * @author yechiel
 * @since 11.0
 */
@com.gigaspaces.api.InternalApi
public class BlobStoreErrorBulkEntryInfo {
    //NOTE- currently used only for update/change
    private final short _originalBlobStoreVersion;
    private final IEntryData _originalEntryData;

    public BlobStoreErrorBulkEntryInfo(Context context) {
        _originalBlobStoreVersion = context.getOriginalBlobStoreVersion();
        _originalEntryData = context.getOriginalData();
    }

    public static void setOnContext(Context context, BlobStoreErrorBulkEntryInfo eri) {
        if (eri != null)
            context.setBlobStoreOriginalEntryInfo(eri.getOriginalEntryData(), eri.getOriginalBlobStoreVersion());
    }

    public static boolean isRelevant(int op) {
        switch (op) {
            case SpaceOperations.UPDATE:
                return true;
            default:
                return false;
        }

    }

    public short getOriginalBlobStoreVersion() {
        return _originalBlobStoreVersion;
    }

    public IEntryData getOriginalEntryData() {
        return _originalEntryData;
    }


}
