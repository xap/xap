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
package com.j_spaces.core.cache.blobStore.offheap;

import com.j_spaces.core.cache.blobStore.BlobStoreRefEntryCacheInfo;
import com.j_spaces.core.cache.blobStore.IBlobStoreOffHeapInfo;

public class BlobStoreOffHeapInfoMock implements IBlobStoreOffHeapInfo {

    private long address;
    private String typeName;
    private short typeCode;

    public BlobStoreOffHeapInfoMock() {
        this.address = BlobStoreRefEntryCacheInfo.UNALLOCATED_OFFHEAP_MEMORY;
        this.typeName = "dummy Type Name";
        typeCode = (short) 3;
    }

    @Override
    public void setOffHeapAddress(long address) {
        this.address = address;
    }

    @Override
    public long getOffHeapAddress() {
        return this.address;
    }

    @Override
    public String getTypeName() {
        return  this.typeName;
    }

    @Override
    public short getServerTypeDescCode() {
        return typeCode;
    }
}
