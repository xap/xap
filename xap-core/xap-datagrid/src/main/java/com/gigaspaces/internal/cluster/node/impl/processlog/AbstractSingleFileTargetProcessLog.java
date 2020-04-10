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

package com.gigaspaces.internal.cluster.node.impl.processlog;

import com.gigaspaces.internal.cluster.node.handlers.IReplicationInFacade;
import com.gigaspaces.internal.cluster.node.impl.ReplicationInContext;
import com.gigaspaces.internal.cluster.node.impl.ReplicationLogUtils;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationGroupHistory;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IDataConsumeResult;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketDataConsumer;
import com.gigaspaces.server.blobstore.BlobStoreException;
import com.j_spaces.core.exception.SpaceUnavailableException;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class AbstractSingleFileTargetProcessLog {

    protected final Logger _specificLogger;
    protected final IReplicationPacketDataConsumer _dataConsumer;
    private final IReplicationProcessLogExceptionHandler _exceptionHandler;
    private final IReplicationInFacade _replicationInFacade;
    private final String _name;
    private final String _groupName;
    private final ReplicationInContext _replicationInContext;
    private final IReplicationGroupHistory _groupHistory;
    private final String _sourceLookupName;

    public AbstractSingleFileTargetProcessLog(
            IReplicationPacketDataConsumer dataConsumer,
            IReplicationProcessLogExceptionHandler exceptionHandler,
            IReplicationInFacade replicationInFacade, String name,
            String groupName, String sourceLookupName,
            IReplicationGroupHistory groupHistory) {
        _dataConsumer = dataConsumer;
        _exceptionHandler = exceptionHandler;
        _replicationInFacade = replicationInFacade;
        _name = name;
        _groupName = groupName;
        _sourceLookupName = sourceLookupName;
        _groupHistory = groupHistory;

        _specificLogger = ReplicationLogUtils.createChannelSpecificLogger(_sourceLookupName, _name, _groupName);

        _replicationInContext = createReplicationInContext();
    }

    public ReplicationInContext createReplicationInContext() {
        return new ReplicationInContext(_sourceLookupName, _groupName, _specificLogger, contentRequiredWhileProcessing(), false);
    }

    abstract protected boolean contentRequiredWhileProcessing();

    public <T extends IReplicationPacketData<?>> IReplicationPacketDataConsumer<T> getDataConsumer() {
        return _dataConsumer;
    }

    public ReplicationInContext getReplicationInContext() {
        return _replicationInContext;
    }

    public IReplicationGroupHistory getGroupHistory() {
        return _groupHistory;
    }

    protected String getSourceLookupName() {
        return _sourceLookupName;
    }

    public String getGroupName() {
        return _groupName;
    }

    public IReplicationProcessLogExceptionHandler getExceptionHandler() {
        return _exceptionHandler;
    }

    public IReplicationInFacade getReplicationInFacade() {
        return _replicationInFacade;
    }

    public String toLogMessage() {
        return dumpState();
    }

    public abstract String dumpState();

    public static void throwIfRepetitiveError(IDataConsumeResult prevResult,
                                              IDataConsumeResult consumeResult) throws Exception {
        if (consumeResult.toException() instanceof BlobStoreException) {
            throw consumeResult.toException();
        }

        if (prevResult == null)
            return;

        if (consumeResult.sameFailure(prevResult))
            throw consumeResult.toException();
    }
}
