package com.j_spaces.core.cache.blobStore;

import java.io.Serializable;

/**
 * @author Yael Nahon
 * @since 12.3
 */
public interface IBlobStoreOffHeapInfo extends Serializable{

    void setOffHeapAddress(long address);

    long getOffHeapAddress();

    String getTypeName();

    short getServerTypeDescCode();
}

