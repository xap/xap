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
