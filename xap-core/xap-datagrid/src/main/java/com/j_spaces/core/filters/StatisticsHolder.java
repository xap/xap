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

package com.j_spaces.core.filters;

import com.gigaspaces.cluster.replication.async.mirror.MirrorStatistics;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.lrmi.LRMIInvocationContext;
import com.gigaspaces.serialization.SmartExternalizable;
import com.gigaspaces.server.blobstore.BlobStoreStatistics;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author kimchy
 */
@com.gigaspaces.api.InternalApi
public class StatisticsHolder implements SmartExternalizable {

    private static final long serialVersionUID = 1505093042251118792L;

    final private static int[] OPERATION_CODES =
            {
                    FilterOperationCodes.BEFORE_WRITE,
                    FilterOperationCodes.BEFORE_READ,
                    FilterOperationCodes.BEFORE_TAKE,
                    FilterOperationCodes.BEFORE_NOTIFY,
                    FilterOperationCodes.BEFORE_CLEAN_SPACE,
                    FilterOperationCodes.BEFORE_UPDATE,
                    FilterOperationCodes.AFTER_READ_MULTIPLE,
                    FilterOperationCodes.AFTER_TAKE_MULTIPLE,
                    FilterOperationCodes.BEFORE_NOTIFY_TRIGGER,
                    FilterOperationCodes.AFTER_NOTIFY_TRIGGER,
                    FilterOperationCodes.BEFORE_EXECUTE,
                    FilterOperationCodes.BEFORE_REMOVE,
                    FilterOperationCodes.AFTER_CHANGE
            };

    public static int[] getOperationCodes() {
        return OPERATION_CODES;
    }

    private long timestamp;

    private long[] operationsCount;

    private ReplicationStatistics replicationStatistics;

    private MirrorStatistics mirrorStatistics;

    private BlobStoreStatistics blobStoreStatistics;

    private int processorQueueSize = -1;

    private int notifierQueueSize = -1;

    private RuntimeStatisticsHolder runtimeStatisticsHolder;

    public StatisticsHolder() {
    }

    public StatisticsHolder(long[] operationsCount) {
        this.timestamp = System.currentTimeMillis();
        this.operationsCount = operationsCount;
        this.runtimeStatisticsHolder = new RuntimeStatisticsHolder();
    }

    public long getTimestamp() {
        return timestamp;
    }

    public long[] getOperationsCount() {
        return operationsCount;
    }

    public long getWriteCount() {
        return operationsCount[0];
    }

    public long getReadCount() {
        return operationsCount[1];
    }

    public long getTakeCount() {
        return operationsCount[2];
    }

    public long getNotificationsRegistrationsCount() {
        return operationsCount[3];
    }

    public long getCleanCount() {
        return operationsCount[4];
    }

    public long getUpdateCount() {
        return operationsCount[5];
    }

    public long getReadMultipleCount() {
        return operationsCount[6];
    }

    public long getTakeMultipleCount() {
        return operationsCount[7];
    }

    public long getNotificationsTriggeredCount() {
        return operationsCount[8];
    }

    public long getNotificationsAcksCount() {
        return operationsCount[9];
    }

    public long getTaskExecutionsCount() {
        return operationsCount[10];
    }

    public long getLeaseExpiredOrCanceledCount() {
        return operationsCount[11];
    }

    public long getChangeCount() {
        if (operationsCount.length >= 13)
            return operationsCount[12];

        return 0;
    }

    public void writeExternal(ObjectOutput out) throws IOException {

        final PlatformLogicalVersion version = LRMIInvocationContext.getEndpointLogicalVersion();

        if (version.greaterOrEquals(PlatformLogicalVersion.v12_3_0)){
            writeExternalV5(out);
        }
        else if (version.greaterOrEquals(PlatformLogicalVersion.v9_1_0))
            writeExternalV4(out);
        else
            writeExternalV3(out);
    }

    private void writeExternalV1(ObjectOutput out)
            throws IOException {
        out.writeLong(timestamp);
        out.writeInt(operationsCount.length);
        for (int i = 0; i < operationsCount.length; i++) {
            out.writeLong(operationsCount[i]);
        }

        out.writeObject(replicationStatistics);

    }

    private void writeExternalV2(ObjectOutput out) throws IOException {
        writeExternalV1(out);

        // handle backwards
        out.writeObject(mirrorStatistics);
    }

    private void writeExternalV3(ObjectOutput out) throws IOException {
        writeExternalV2(out);

        out.writeInt(processorQueueSize);
        out.writeInt(notifierQueueSize);
    }

    private void writeExternalV4(ObjectOutput out) throws IOException {
        writeExternalV3(out);
        runtimeStatisticsHolder.writeExternal(out);
    }

    private void writeExternalV5(ObjectOutput out) throws IOException {
        writeExternalV4(out);
        IOUtils.writeObject(out, blobStoreStatistics);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        final PlatformLogicalVersion version = LRMIInvocationContext.getEndpointLogicalVersion();

        if (version.greaterOrEquals(PlatformLogicalVersion.v12_3_0)){
            readExternalV5(in);
        }
        else if (version.greaterOrEquals(PlatformLogicalVersion.v9_1_0))
            readExternalV4(in);
        else
            readExternalV3(in);
    }

    public void readExternalV1(ObjectInput in) throws IOException, ClassNotFoundException {
        timestamp = in.readLong();
        operationsCount = new long[in.readInt()];
        for (int i = 0; i < operationsCount.length; i++) {
            operationsCount[i] = in.readLong();
        }

        replicationStatistics = (ReplicationStatistics) in.readObject();
    }

    public void readExternalV2(ObjectInput in) throws IOException, ClassNotFoundException {
        readExternalV1(in);
        mirrorStatistics = (MirrorStatistics) in.readObject();
    }

    private void readExternalV3(ObjectInput in) throws IOException, ClassNotFoundException {
        readExternalV2(in);

        processorQueueSize = in.readInt();
        notifierQueueSize = in.readInt();
    }

    private void readExternalV4(ObjectInput in) throws IOException, ClassNotFoundException {
        readExternalV3(in);
        runtimeStatisticsHolder = new RuntimeStatisticsHolder();
        runtimeStatisticsHolder.readExternal(in);
    }

    private void readExternalV5(ObjectInput in) throws IOException, ClassNotFoundException {
        readExternalV4(in);
        blobStoreStatistics = IOUtils.readObject( in );
    }

    public ReplicationStatistics getReplicationStatistics() {
        return replicationStatistics;
    }

    public void setReplicationStatistics(ReplicationStatistics replicationStatistics) {
        this.replicationStatistics = replicationStatistics;
    }

    public MirrorStatistics getMirrorStatistics() {
        return mirrorStatistics;
    }

    public void setMirrorStatistics(MirrorStatistics mirrorStatistics) {
        this.mirrorStatistics = mirrorStatistics;
    }

    public BlobStoreStatistics getBlobStoreStatistics() {
        return blobStoreStatistics;
    }

    public void setBlobStoreStatistics(BlobStoreStatistics blobStoreStatistics) {
        this.blobStoreStatistics = blobStoreStatistics;
    }

    public void setProcessorQueueSize(int processorQueueSize) {
        this.processorQueueSize = processorQueueSize;
    }

    public int getProcessorQueueSize() {
        return processorQueueSize;
    }

    public void setNotifierQueueSize(int notifierQueueSize) {
        this.notifierQueueSize = notifierQueueSize;
    }

    public int getNotifierQueueSize() {
        return notifierQueueSize;
    }

    public void setRuntimeStatisticsHolder(RuntimeStatisticsHolder runtimeStatisticsHolder) {
        this.runtimeStatisticsHolder = runtimeStatisticsHolder;
    }

    public RuntimeStatisticsHolder getRuntimeStatisticsHolder() {
        return runtimeStatisticsHolder;
    }
}
