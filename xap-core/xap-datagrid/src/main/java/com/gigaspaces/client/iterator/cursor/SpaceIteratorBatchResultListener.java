package com.gigaspaces.client.iterator.cursor;

import com.gigaspaces.async.AsyncFutureListener;
import com.gigaspaces.async.AsyncResult;
import com.gigaspaces.internal.client.SpaceIteratorBatchResult;

/**
 * @author Alon Shoham
 * @since 15.2.0
 */
@com.gigaspaces.api.InternalApi
public class SpaceIteratorBatchResultListener implements AsyncFutureListener<SpaceIteratorBatchResult> {
    private final SpaceIteratorBatchResultProvider _spaceIteratorBatchResultProvider;

    public SpaceIteratorBatchResultListener(SpaceIteratorBatchResultProvider spaceIteratorBatchResultProvider) {
        _spaceIteratorBatchResultProvider = spaceIteratorBatchResultProvider;
    }

    @Override
    public void onResult(AsyncResult<SpaceIteratorBatchResult> result) {
        _spaceIteratorBatchResultProvider.addAsyncBatchResult(result);
    }
}
