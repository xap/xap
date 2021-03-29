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

package com.gigaspaces.internal.cluster.node.impl.handlers;

import com.gigaspaces.client.mutators.SpaceEntryMutator;
import com.gigaspaces.internal.cluster.node.IReplicationInContext;
import com.gigaspaces.internal.cluster.node.ReplicationBlobstoreBulkContext;
import com.gigaspaces.internal.server.space.SpaceEngine;
import com.gigaspaces.internal.server.storage.IEntryData;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.j_spaces.core.OperationID;
import com.j_spaces.core.UnknownTypeException;
import com.j_spaces.core.cache.blobStore.BlobStoreReplicationBulkConsumeHelper;

import net.jini.core.entry.UnusableEntryException;
import net.jini.core.transaction.Transaction;
import net.jini.core.transaction.TransactionException;

import java.rmi.RemoteException;
import java.util.Collection;

import org.slf4j.Logger;

/**
 * @author Boris
 * @since 11.0.0 Handles incoming blobstore bulks, flushes the previous bulk if a new bulk entry
 * arrived or non bulk entry arrived.
 */
@com.gigaspaces.api.InternalApi
public class BlobstorePrimaryBackupSpaceReplicationEntryEventHandler
        extends PrimaryBackupSpaceReplicationEntryEventHandler {

    private static final String LOG_PREFIX = "[BlobstoreReplicationEntryHandler]";

    public BlobstorePrimaryBackupSpaceReplicationEntryEventHandler(SpaceEngine engine, boolean isCentralAndExternalDB) {
        super(engine, isCentralAndExternalDB);
    }

    @Override
    protected void writeEntryIntoSpace(IReplicationInContext context,
                                       Transaction txn, IEntryPacket entry, long lease, boolean twoPhaseCommit) throws Exception {
        handleBlobstoreReplicationBulkIfNeeded(context, false);
        super.writeEntryIntoSpace(context, txn, entry, lease, twoPhaseCommit);
    }

    /**
     * Flushes the bulk to disk after consuming the replication.
     */
    @Override
    public void afterConsumption(IReplicationInContext context, boolean successful, long lastProcessedKey) {
        if (_engine.getReplicationNode().getBlobStoreReplicationBulkConsumeHelper() != null) {
            _engine.getReplicationNode().getBlobStoreReplicationBulkConsumeHelper().flushBulk(lastProcessedKey);
            logFlushAfterConsumption(context, successful, lastProcessedKey);
        }
    }

    @Override
    public void beforeConsume(IReplicationInContext context) {
        ReplicationBlobstoreBulkContext replicationBlobstoreBulkContext = context.getReplicationBlobstoreBulkContext();
        BlobStoreReplicationBulkConsumeHelper blobStoreReplicationBulkConsumeHelper = _engine.getReplicationNode().getBlobStoreReplicationBulkConsumeHelper();
        if (blobStoreReplicationBulkConsumeHelper != null && replicationBlobstoreBulkContext != null) {
            if (replicationBlobstoreBulkContext.getBlobStoreReplicationBulkConsumeHelper() == null) {
                replicationBlobstoreBulkContext.setBlobStoreReplicationBulkConsumeHelper(blobStoreReplicationBulkConsumeHelper);
            }
        }
    }

    private void handleBlobstoreReplicationBulkIfNeeded(IReplicationInContext context, boolean take) {
        ReplicationBlobstoreBulkContext replicationBlobstoreBulkContext = context.getReplicationBlobstoreBulkContext();
        BlobStoreReplicationBulkConsumeHelper blobStoreReplicationBulkConsumeHelper = _engine.getReplicationNode().getBlobStoreReplicationBulkConsumeHelper();
        if (replicationBlobstoreBulkContext != null && blobStoreReplicationBulkConsumeHelper != null) {
            if (replicationBlobstoreBulkContext.isPartOfBlobstoreBulk()) {
                // store blobstore bulk info if not exists to be used for bulking
                if (!replicationBlobstoreBulkContext.shouldFlush()) {
                    logProcessingBulk(context);
                    blobStoreReplicationBulkConsumeHelper.prepareForBulking(take);
                }
                // if a new bulk id arrived
                else {
                    logNewBulkArrived(context);
                    // flush the previous bulk if exists
                    blobStoreReplicationBulkConsumeHelper.flushBulk(context.getLastProcessedKey());
                    // prepare new bulk info because a new bulk id arrived
                    blobStoreReplicationBulkConsumeHelper.prepareForBulking(take);
                }
            }
            // non bulk entry arrived, flush the previous bulk if exists
            else {
                logNonBulkEntryArrived(context);
                blobStoreReplicationBulkConsumeHelper.flushBulk(context.getLastProcessedKey());
            }
        }
    }

    private void logNewBulkArrived(IReplicationInContext context) {
        Logger contextLogger = context.getContextLogger();
        int bulkId = context.getReplicationBlobstoreBulkContext().getBulkId();
        if (contextLogger != null && contextLogger.isTraceEnabled()) {
            contextLogger.trace(LOG_PREFIX + " an entry which is a part of a new blobstore bulk with id [" + bulkId + "] arrived," +
                    " will flush the previous bulk, packetkey=" + context.getLastProcessedKey());
        }
    }

    private void logNonBulkEntryArrived(IReplicationInContext context) {
        Logger contextLogger = context.getContextLogger();
        if (contextLogger != null && contextLogger.isTraceEnabled()) {
            contextLogger.trace(LOG_PREFIX + " an entry which is not part of a blobstore bulk arrived, " +
                    "will flush the previous bulk, packetKey=" + context.getLastProcessedKey());
        }
    }

    private void logProcessingBulk(IReplicationInContext context) {
        Logger contextLogger = context.getContextLogger();
        int bulkId = context.getReplicationBlobstoreBulkContext().getBulkId();
        if (contextLogger != null && contextLogger.isTraceEnabled()) {
            contextLogger.trace(LOG_PREFIX + " processing replication blobstore bulk with id [" + bulkId + "], packetKey=" + context.getLastProcessedKey());
        }
    }

    private void logFlushAfterConsumption(IReplicationInContext context, boolean successful, long lastProcessedKey) {
        if (context.getContextLogger() != null && context.getContextLogger().isDebugEnabled()) {
            int bulkId = 0;
            if (context.getReplicationBlobstoreBulkContext() != null) {
                bulkId = context.getReplicationBlobstoreBulkContext().getBulkId();
            }
            String bulkIdStr = Integer.toString(bulkId).equals("0") ? "none" : Integer.toString(bulkId);
            context.getContextLogger().debug(LOG_PREFIX + " flushed the bulk [" + bulkIdStr + "] after consumption," +
                    " successful? [" + successful + "], last flushed key=" + lastProcessedKey + ", thread=" + Thread.currentThread().getName());
        }
    }

    @Override
    protected void updateEntryInSpace(IReplicationInContext context,
                                      Transaction txn, IEntryPacket entry, IEntryPacket previousEntry, boolean partialUpdate,
                                      boolean overrideVersion, long lease, boolean twoPhaseCommit) throws UnusableEntryException,
            UnknownTypeException, TransactionException, RemoteException {
        handleBlobstoreReplicationBulkIfNeeded(context, false);
        super.updateEntryInSpace(context,
                txn,
                entry,
                previousEntry,
                partialUpdate,
                overrideVersion,
                lease,
                twoPhaseCommit);
    }

    @Override
    protected void changeEntryInSpace(IReplicationInContext context,
                                      Transaction txn, String uid, int version,
                                      Collection<SpaceEntryMutator> mutators, boolean isTransient,
                                      OperationID operationID, IEntryData previousEntry, long timeToLive,
                                      boolean twoPhaseCommit, String typeName) throws Exception {
        handleBlobstoreReplicationBulkIfNeeded(context, false);
        super.changeEntryInSpace(context,
                txn,
                uid,
                version,
                mutators,
                isTransient,
                operationID,
                previousEntry,
                timeToLive,
                twoPhaseCommit, typeName);
    }

    @Override
    protected void removeEntryFromSpace(IReplicationInContext context,
                                        Transaction txn, ITemplatePacket template, boolean twoPhaseCommit)
            throws TransactionException, UnusableEntryException,
            UnknownTypeException, RemoteException {
        handleBlobstoreReplicationBulkIfNeeded(context, true);
        super.removeEntryFromSpace(context, txn, template, twoPhaseCommit);
    }

}
