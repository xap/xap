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

import com.gigaspaces.datasource.BulkItem;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.lrmi.LRMIInvocationContext;
import com.gigaspaces.metrics.MetricRegistrator;
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

/**
 * @author anna
 * @since 7.1.1
 */
@com.gigaspaces.api.InternalApi
public class MirrorOperationsImpl extends AbstractMirrorOperations implements MirrorOperations, SmartExternalizable {
    private final static long serialVersionUID = 1L;

    private MirrorOperationStatisticsImpl _writeOperationStatistics = new MirrorOperationStatisticsImpl();
    private MirrorOperationStatisticsImpl _updateOperationStatistics = new MirrorOperationStatisticsImpl();
    private MirrorOperationStatisticsImpl _removeOperationStatistics = new MirrorOperationStatisticsImpl();
    private MirrorOperationStatisticsImpl _changeOperationStatistics = new MirrorOperationStatisticsImpl();

    public MirrorOperationsImpl() {
        super();
    }

    @Override
    public MirrorOperationStatisticsImpl getWriteOperationStatistics() {
        return _writeOperationStatistics;
    }

    @Override
    public MirrorOperationStatisticsImpl getUpdateOperationStatistics() {
        return _updateOperationStatistics;
    }

    @Override
    public MirrorOperationStatisticsImpl getRemoveOperationStatistics() {
        return _removeOperationStatistics;
    }

    @Override
    public MirrorOperationStatisticsImpl getChangeOperationStatistics() {
        return _changeOperationStatistics;
    }

    public void addOperationCount(List<BulkItem> bulkItems) {
        for (BulkItem item : bulkItems)
            getOperationStatistics(item.getOperation()).incrementOperationCount();
    }

    public void addSuccessfulOperationCount(List<BulkItem> bulkItems) {
        for (BulkItem item : bulkItems)
            getOperationStatistics(item.getOperation()).incrementSuccessfulOperationCount();
    }

    public void addFailedOperationCount(List<BulkItem> bulkItems) {
        for (BulkItem item : bulkItems)
            getOperationStatistics(item.getOperation()).incrementFailedOperationCount();
    }

    public void register(MetricRegistrator metricRegistrator) {
        _writeOperationStatistics.register(metricRegistrator, "write-operations-");
        _updateOperationStatistics.register(metricRegistrator, "update-operations-");
        _removeOperationStatistics.register(metricRegistrator, "remove-operations-");
        _changeOperationStatistics.register(metricRegistrator, "change-operations-");
    }

    private MirrorOperationStatisticsImpl getOperationStatistics(short itemCode) {
        switch (itemCode) {
            case BulkItem.WRITE:
                return getWriteOperationStatistics();
            case BulkItem.UPDATE:
            case BulkItem.PARTIAL_UPDATE:
                return getUpdateOperationStatistics();
            case BulkItem.REMOVE:
                return getRemoveOperationStatistics();
            case BulkItem.CHANGE:
                return getChangeOperationStatistics();
            default:
                throw new IllegalArgumentException("Unsupported item code: " + itemCode);
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(_writeOperationStatistics);
        out.writeObject(_updateOperationStatistics);
        out.writeObject(_removeOperationStatistics);
        if (LRMIInvocationContext.getEndpointLogicalVersion().greaterOrEquals(PlatformLogicalVersion.v9_5_0))
            out.writeObject(_changeOperationStatistics);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        _writeOperationStatistics = ((MirrorOperationStatisticsImpl) in.readObject());
        _updateOperationStatistics = ((MirrorOperationStatisticsImpl) in.readObject());
        _removeOperationStatistics = ((MirrorOperationStatisticsImpl) in.readObject());
        if (LRMIInvocationContext.getEndpointLogicalVersion().greaterOrEquals(PlatformLogicalVersion.v9_5_0))
            _changeOperationStatistics = ((MirrorOperationStatisticsImpl) in.readObject());
    }
}
