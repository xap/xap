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

package com.gigaspaces.internal.cluster.node.impl.processlog.globalorder;

import com.gigaspaces.internal.cluster.node.IReplicationInBatchContext;
import com.gigaspaces.internal.cluster.node.impl.AbstractReplicationInBatchContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@com.gigaspaces.api.InternalApi
public class GlobalOrderReplicationInBatchContext
        extends AbstractReplicationInBatchContext implements IReplicationInBatchContext {
    private final IBatchExecutedCallback _batchExecutedCallback;

    public GlobalOrderReplicationInBatchContext(
            IBatchExecutedCallback batchExecutedCallback, Logger contextLogger, int entireBatchSize, String sourceLookupName, String groupName) {
        super(contextLogger, entireBatchSize, sourceLookupName, groupName, false);
        _batchExecutedCallback = batchExecutedCallback;
    }

    @Override
    protected void afterBatchConsumed(long lastProcessedKeyInBatch) {
        _batchExecutedCallback.batchConsumed(lastProcessedKeyInBatch);
    }

}
