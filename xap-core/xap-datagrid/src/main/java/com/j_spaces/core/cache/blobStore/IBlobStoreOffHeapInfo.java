package com.j_spaces.core.cache.blobStore;

/**
 * @author Yael Nahon
 * @since 12.3
 */
public interface IBlobStoreOffHeapInfo {

    void setOffHeapAddress(long address);

    long getOffHeapAddress();
}

