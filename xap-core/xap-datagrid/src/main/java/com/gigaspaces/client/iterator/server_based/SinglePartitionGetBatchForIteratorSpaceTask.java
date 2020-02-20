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
package com.gigaspaces.client.iterator.server_based;

import com.gigaspaces.executor.SpaceTask;
import com.gigaspaces.internal.client.SpaceIteratorBatchResult;
import com.j_spaces.core.IJSpace;
import net.jini.core.transaction.Transaction;

/**
 * @author Alon Shoham
 * @since 15.2.0
 */
@com.gigaspaces.api.InternalApi
public class SinglePartitionGetBatchForIteratorSpaceTask implements SpaceTask<SpaceIteratorBatchResult> {
    private final SpaceIteratorBatchResultProvider _spaceIteratorBatchResultProvider;
    private final boolean _firstTime;

    SinglePartitionGetBatchForIteratorSpaceTask(SpaceIteratorBatchResultProvider spaceIteratorBatchResultProvider, boolean firstTime) {
        _spaceIteratorBatchResultProvider = spaceIteratorBatchResultProvider;
        _firstTime = firstTime;
    }

    @Override
    public SpaceIteratorBatchResult execute(IJSpace space, Transaction tx) throws Exception {
        return space.getDirectProxy().getBatchForIterator(_spaceIteratorBatchResultProvider.getQueryPacket(), _spaceIteratorBatchResultProvider.getBatchSize(), _spaceIteratorBatchResultProvider.getReadModifiers(), _spaceIteratorBatchResultProvider.getUuid(), _firstTime);
    }

}
