package com.gigaspaces.client.iterator.cursor;

import com.gigaspaces.api.InternalApi;

@InternalApi
public interface SpaceIteratorBatchProvider {
    Object[] getNextBatch() throws InterruptedException;

    void close();
}
