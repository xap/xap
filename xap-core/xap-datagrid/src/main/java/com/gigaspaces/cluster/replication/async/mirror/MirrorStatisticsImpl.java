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

package com.gigaspaces.cluster.replication.async.mirror;

import com.gigaspaces.internal.cluster.node.handlers.IReplicationInOperationsStatistics;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.metrics.DummyMetricRegistrator;
import com.gigaspaces.metrics.MetricConstants;
import com.gigaspaces.metrics.MetricRegistrator;
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Default implementation of the mirror space statistics. Contains the operation statistics for all
 * the cluster operations and also statistcis per channel.
 *
 * @author anna
 * @since 7.1.1
 */
@com.gigaspaces.api.InternalApi
public class MirrorStatisticsImpl extends AbstractMirrorOperations implements MirrorStatistics, SmartExternalizable,
        IReplicationInOperationsStatistics<MirrorOperations> {
    private static final long serialVersionUID = 1L;

    private ConcurrentHashMap<String, MirrorOperationsImpl> _channelStatistics = new ConcurrentHashMap<String, MirrorOperationsImpl>();
    private transient MetricRegistrator _metricRegistrator = DummyMetricRegistrator.get();

    public MirrorStatisticsImpl() {
        super();
    }

    @Override
    public String toString() {
        return "MirrorStatistics [\n operationCount="
                + getOperationCount() + ",\n successfulOperationCount="
                + getSuccessfulOperationCount()
                + ", \n discardedOperationCount="
                + getDiscardedOperationCount() + ", \n failedOperationCount="
                + getFailedOperationCount()
                + ", \n inProgressOperationCount="
                + getInProgressOperationCount()
                + ",\n writeOperationStatistics="
                + getWriteOperationStatistics()
                + ", updateOperationStatistics="
                + getUpdateOperationStatistics()
                + ", removeOperationStatistics="
                + getRemoveOperationStatistics()
                + ", changeOperationStatistics="
                + getChangeOperationStatistics()
                + ", channelStatistics="
                + getAllSourceChannelStatistics() + "]";
    }

    public void setMetricRegistrator(MetricRegistrator metricRegistrator) {
        this._metricRegistrator = metricRegistrator.extend(MetricConstants.MIRROR_METRIC_NAME);
    }

    public Map<String, ? extends MirrorOperations> getAllSourceChannelStatistics() {
        return _channelStatistics;
    }

    public MirrorOperations getSourceChannelStatistics(String channelName) {
        MirrorOperationsImpl channelStat = _channelStatistics.get(channelName);

        if (channelStat == null) {
            channelStat = new MirrorOperationsImpl();
            channelStat.register(_metricRegistrator.extend(SpaceImpl.extractInstanceId(channelName)));
            _channelStatistics.putIfAbsent(channelName, channelStat);

        }
        return channelStat;
    }

    @Override
    public MirrorOperationStatistics getWriteOperationStatistics() {
        MirrorOperationStatisticsImpl stat = new MirrorOperationStatisticsImpl();
        for (AbstractMirrorOperations channelStat : _channelStatistics.values())
            stat.add(channelStat.getWriteOperationStatistics());
        return stat;
    }

    @Override
    public MirrorOperationStatistics getUpdateOperationStatistics() {
        MirrorOperationStatisticsImpl stat = new MirrorOperationStatisticsImpl();
        for (AbstractMirrorOperations channelStat : _channelStatistics.values())
            stat.add(channelStat.getUpdateOperationStatistics());
        return stat;
    }

    @Override
    public MirrorOperationStatistics getRemoveOperationStatistics() {
        MirrorOperationStatisticsImpl stat = new MirrorOperationStatisticsImpl();
        for (AbstractMirrorOperations channelStat : _channelStatistics.values())
            stat.add(channelStat.getRemoveOperationStatistics());
        return stat;
    }

    @Override
    public MirrorOperationStatistics getChangeOperationStatistics() {
        MirrorOperationStatisticsImpl stat = new MirrorOperationStatisticsImpl();
        for (AbstractMirrorOperations channelStat : _channelStatistics.values())
            stat.add(channelStat.getChangeOperationStatistics());
        return stat;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        //The way we serialize object will throw exception if we write a concurrent hashmap in java 1.6
        //and the target reads it in java 1.5, we replace the map before sending with regular hashmap to avoid this
        out.writeObject(new HashMap<String, MirrorOperationsImpl>(_channelStatistics));
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        //The way we serialize object will throw exception if we write a concurrent hashmap in java 1.6
        //and the target reads it in java 1.5, we replace the map before sending with regular hashmap to avoid this
        _channelStatistics = new ConcurrentHashMap<String, MirrorOperationsImpl>((HashMap<String, MirrorOperationsImpl>) in.readObject());
    }
}
