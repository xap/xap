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
