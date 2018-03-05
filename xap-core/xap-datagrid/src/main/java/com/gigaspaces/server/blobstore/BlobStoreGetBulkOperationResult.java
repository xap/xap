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

package com.gigaspaces.server.blobstore;


import com.j_spaces.core.cache.blobStore.IBlobStoreOffHeapInfo;

/*******************************************************************************
 * Copyright (c) 2010 GigaSpaces Technologies Ltd. All rights reserved
 *
 * The software source code is proprietary and confidential information of GigaSpaces. You may use
 * the software source code solely under the terms and limitations of The license agreement granted
 * to you by GigaSpaces.
 *******************************************************************************/
//

@com.gigaspaces.api.InternalApi
public class BlobStoreGetBulkOperationResult extends BlobStoreBulkOperationResult {

    private IBlobStoreOffHeapInfo offHeapInfo;

    public BlobStoreGetBulkOperationResult(java.io.Serializable id, java.io.Serializable data, Object position, IBlobStoreOffHeapInfo offHeapInfo) {
        super(BlobStoreBulkOperationType.GET, id, data, position);
        this.offHeapInfo = offHeapInfo;
    }

    public BlobStoreGetBulkOperationResult(java.io.Serializable id, Throwable exception) {
        super(BlobStoreBulkOperationType.GET, id, exception);
    }

    @Override
    public IBlobStoreOffHeapInfo getOffHeapInfo() {
        return offHeapInfo;
    }
}


