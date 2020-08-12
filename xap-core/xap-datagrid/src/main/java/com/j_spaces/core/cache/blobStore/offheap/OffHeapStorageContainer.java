package com.j_spaces.core.cache.blobStore.offheap;

import com.j_spaces.core.cache.blobStore.memory_pool.AbstractMemoryPool;

public interface OffHeapStorageContainer {
    AbstractMemoryPool getMemoryPool();
}
