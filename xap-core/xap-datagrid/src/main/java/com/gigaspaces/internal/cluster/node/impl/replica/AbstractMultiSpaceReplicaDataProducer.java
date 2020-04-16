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

package com.gigaspaces.internal.cluster.node.impl.replica;

import com.gigaspaces.internal.cluster.node.replica.SpaceCopyReplicaParameters;
import com.gigaspaces.internal.server.space.SpaceEngine;
import com.gigaspaces.internal.utils.StringUtils;
import com.gigaspaces.logger.Constants;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Abstraction for a data producers that is a composition of multiple data producers.
 *
 * @author anna
 * @version 1.0
 * @since 8.0
 */
public abstract class AbstractMultiSpaceReplicaDataProducer
        implements ISpaceReplicaDataProducer<IExecutableSpaceReplicaData> {

    protected final static Logger _logger = LoggerFactory.getLogger(Constants.LOGGER_REPLICATION_REPLICA);

    private final List<ISingleStageReplicaDataProducer<? extends IExecutableSpaceReplicaData>> _dataProducers;
    protected final Object _requestContext;
    protected final SpaceEngine _engine;
    private volatile boolean _closed;
    private volatile int _currentProducer;
    private AtomicBoolean _closing;

    public AbstractMultiSpaceReplicaDataProducer(SpaceEngine engine,
                                                 SpaceCopyReplicaParameters parameters, Object requestContext) {
        _engine = engine;
        _requestContext = requestContext;
        _dataProducers = buildDataProducers(parameters);
        _closing = new AtomicBoolean(false);
    }

    protected abstract List<ISingleStageReplicaDataProducer<? extends IExecutableSpaceReplicaData>> buildDataProducers(
            SpaceCopyReplicaParameters parameters);

    public IExecutableSpaceReplicaData produceNextData(ISynchronizationCallback synchCallback) {
        if (_closed)
            return null;

        if (_currentProducer >= _dataProducers.size())
            return null;

        IExecutableSpaceReplicaData replicaData = _dataProducers.get(_currentProducer)
                .produceNextData(synchCallback);
        return replicaData;
    }

    /*public synchronized void close()
    {
        if (_closed)
            return;
        _closed = true;

        for (ISingleStageReplicaDataProducer<? extends IExecutableSpaceReplicaData> dataProducer : _dataProducers)
            dataProducer.close();

        onClose();
    }*/

    /**
     * @return true if any thread is trying to close and the lease reaper thread is trying to close
     * at the same time.
     */
    public CloseStatus close(boolean forced) {
        if (!_closing.compareAndSet(false, true) && _engine.getLeaseManager().isCurrentLeaseReaperThread()) {
            return CloseStatus.CLOSING;
        }
        synchronized (this) {
            if (_closed)
                return CloseStatus.CLOSED;
            _closed = true;

            for (ISingleStageReplicaDataProducer<? extends IExecutableSpaceReplicaData> dataProducer : _dataProducers)
                dataProducer.close(forced);

            onClose();
        }
        _closing.set(false);
        return CloseStatus.CLOSED;
    }

    protected void onClose() {
    }

    public CurrentStageInfo nextReplicaStage() {
        boolean isLast = _currentProducer == (_dataProducers.size() - 1);
        String currentProducerName = _dataProducers.get(_currentProducer).getName();
        if (!isLast)
            _currentProducer++;
        String nextProducerName = isLast ? null : _dataProducers.get(_currentProducer).getName();

        return new CurrentStageInfo(currentProducerName, nextProducerName, isLast);
    }

    protected String getLogPrefix() {
        return _engine.getReplicationNode() + "context [" + _requestContext + "] ";
    }

    public String dumpState() {
        StringBuilder dump = new StringBuilder("Stages: ");
        dump.append(StringUtils.NEW_LINE);
        for (ISingleStageReplicaDataProducer<? extends IExecutableSpaceReplicaData> stage : _dataProducers) {
            dump.append(stage.dumpState());
            dump.append(StringUtils.NEW_LINE);
        }
        return dump.toString();
    }
}
