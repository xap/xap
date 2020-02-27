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

package com.gigaspaces.internal.cluster.node.impl.packets.data;


import com.gigaspaces.internal.cluster.node.IReplicationInContext;
import com.gigaspaces.internal.cluster.node.ReplicationBlobstoreBulkContext;
import com.gigaspaces.internal.cluster.node.handlers.IReplicationInFacade;
import com.gigaspaces.internal.cluster.node.impl.filters.IReplicationInFilterCallback;
import com.gigaspaces.internal.cluster.node.impl.packets.data.operations.AbstractReplicationPacketSingleEntryData;
import com.gigaspaces.internal.server.space.metadata.SpaceTypeManager;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Boris
 * @since 11.0.0 Consumes packets and adds the proper information to replication context about
 * whether the packet is part of blobstore bulk or not.
 */
@com.gigaspaces.api.InternalApi
public class BlobstoreReplicationPacketDataConsumer extends ReplicationPacketDataConsumer {
    private static final String LOG_PREFIX = "[BlobstoreReplicationConsumer]";

    public BlobstoreReplicationPacketDataConsumer(SpaceTypeManager typeManager, IDataConsumeFixFacade fixFacade, ReplicationPacketDataMediator packetDataMediator) {
        super(typeManager, fixFacade, packetDataMediator);
    }

    public IDataConsumeResult consume(IReplicationInContext context,
                                      IExecutableReplicationPacketData<?> data,
                                      IReplicationInFacade replicationInFacade,
                                      IReplicationInFilterCallback inFilterCallback) {
        // inject BlobStoreReplicationBulkConsumeHelper to context
        replicationInFacade.beforeConsume(context);
        prepareBlobstoreBulkIfNeeded(context, data);
        return super.consume(context, data, replicationInFacade, inFilterCallback);
    }

    private void prepareBlobstoreBulkIfNeeded(IReplicationInContext context, IExecutableReplicationPacketData<?> data) {
        if (data.isPartOfBlobstoreBulk()) {
            AbstractReplicationPacketSingleEntryData singleEntryData = ((AbstractReplicationPacketSingleEntryData) data);
            handleBulkEntryArrived(context, singleEntryData);
        }
        // entry arrived and it is not bulk, should flush the previous bulk if exist
        else {
            handleNonBulkEntryArrived(context, data);
        }
    }

    private void handleBulkEntryArrived(IReplicationInContext context, AbstractReplicationPacketSingleEntryData singleEntryData) {
        logBulkEntryArrived(context, singleEntryData);
        if (!initReplicationBulkContextIfNeeded(context, singleEntryData.getBlobstoreBulkId())) {
            // continue processing the previous bulk or a new bulk arrived
            context.getReplicationBlobstoreBulkContext().setBulkReplicationInfo(singleEntryData.getBlobstoreBulkId());
        }
    }

    private void handleNonBulkEntryArrived(IReplicationInContext context, IExecutableReplicationPacketData singleEntryData) {
        // previous operation was part of a bulk, flush it and mark it to be cleared later
        logNonBulkEntryArrived(context, singleEntryData);
        initReplicationBulkContextIfNeeded(context, 0);
        context.getReplicationBlobstoreBulkContext().nonBulkArrived();
    }

    private boolean initReplicationBulkContextIfNeeded(IReplicationInContext context, int bulkId) {
        if (context.getReplicationBlobstoreBulkContext() == null) {
            ReplicationBlobstoreBulkContext bulkContext = new ReplicationBlobstoreBulkContext(bulkId);
            context.setReplicationBlobstoreBulkContext(bulkContext);
            return true;
        }
        return false;
    }

    private void logNonBulkEntryArrived(IReplicationInContext context, IExecutableReplicationPacketData singleEntryData) {
        Logger contextLogger = context.getContextLogger();
        ReplicationBlobstoreBulkContext replicationBlobstoreBulkContext = context.getReplicationBlobstoreBulkContext();
        if (contextLogger != null && contextLogger.isLoggable(Level.FINEST)) {
            String uid = "multiUid";
            if (singleEntryData.isSingleEntryData()) {
                uid = singleEntryData.getSingleEntryData().getUid();
            }
            if (replicationBlobstoreBulkContext != null) {
                contextLogger.log(Level.FINEST, LOG_PREFIX + " handling incoming non blobstore bulk entry with uid [" + uid + "]," +
                        " will ask to flush the previous bulk with id [" + replicationBlobstoreBulkContext.getBulkId() + "], " +
                        "thread=" + Thread.currentThread().getName() + ", packetKey=" + context.getLastProcessedKey());
            } else {
                contextLogger.log(Level.FINEST, LOG_PREFIX + " handling incoming non blobstore bulk entry with uid [" + uid + "]," +
                        " the previous entry was not a part of a blobstore bulk, thread=" + Thread.currentThread().getName()
                        + ", packetKey=" + context.getLastProcessedKey());
            }
        }
    }

    private void logBulkEntryArrived(IReplicationInContext context, AbstractReplicationPacketSingleEntryData singleEntryData) {
        Logger contextLogger = context.getContextLogger();
        ReplicationBlobstoreBulkContext bulkContext = context.getReplicationBlobstoreBulkContext();
        if (contextLogger != null && contextLogger.isLoggable(Level.FINEST)) {
            if (bulkContext == null) {
                contextLogger.log(Level.FINEST, LOG_PREFIX + " handling first incoming blobstore bulk with id [" + singleEntryData.getBlobstoreBulkId() + "]" +
                        ", entry uid=" + singleEntryData.getUid() + ", operation type=" + singleEntryData.getOperationType() +
                        ", thread=" + Thread.currentThread().getName() + ", packetKey=" + context.getLastProcessedKey());
            } else {
                contextLogger.log(Level.FINEST, LOG_PREFIX + " handling incoming ongoing blobstore bulk with id [" + singleEntryData.getBlobstoreBulkId() + "]" +
                        ", entry uid=" + singleEntryData.getUid() + ", operation type=" + singleEntryData.getOperationType() +
                        ", thread=" + Thread.currentThread().getName() + ", packetKey=" + context.getLastProcessedKey());
            }
        }
    }
}
