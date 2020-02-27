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

import com.gigaspaces.internal.cluster.node.IReplicationInContext;
import com.gigaspaces.internal.cluster.node.handlers.IReplicationInEntryLeaseCancelledHandler;
import com.gigaspaces.internal.cluster.node.handlers.IReplicationInEntryLeaseExpiredHandler;
import com.gigaspaces.internal.cluster.node.handlers.IReplicationInEntryLeaseExtendedHandler;
import com.gigaspaces.internal.server.space.SpaceEngine;
import com.gigaspaces.internal.server.storage.IEntryData;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.logger.LogLevel;
import com.j_spaces.core.LeaseManager;
import com.j_spaces.core.ObjectTypes;
import com.j_spaces.core.OperationID;
import com.j_spaces.core.cluster.ConflictingOperationPolicy;

import net.jini.core.lease.UnknownLeaseException;

import java.util.logging.Level;

/**
 * @author Niv Ingberg
 * @since 8.0.5
 */
@com.gigaspaces.api.InternalApi
public class SpaceReplicationEntryLeaseEventHandler implements
        IReplicationInEntryLeaseCancelledHandler,
        IReplicationInEntryLeaseExtendedHandler,
        IReplicationInEntryLeaseExpiredHandler {

    protected final SpaceEngine _engine;
    private final boolean _isPrimaryBackupSpace;

    public SpaceReplicationEntryLeaseEventHandler(SpaceEngine engine) {
        this._engine = engine;
        _isPrimaryBackupSpace = _engine.getClusterPolicy() != null && _engine.getClusterPolicy().isPrimaryElectionAvailable();
    }

    // TODO: use final member instead of lazy getter when LeaseManager construction is refactored.
    protected LeaseManager getLeaseManager() {
        return _engine.getLeaseManager();
    }

    // TODO: use final member instead of lazy getter when ConflictingOperationPolicy construction is refactored.
    private ConflictingOperationPolicy getConflictingOperationPolicy() {
        return _engine.getConflictingOperationPolicy();
    }

    private boolean ignoreOperation(boolean isTransient) {
        return !isTransient && _engine.getCacheManager().isCentralDB() && _engine.getCacheManager().isCacheExternalDB() && _engine.getCacheManager().isEvictableCachePolicy();
    }

    @Override
    public void inCancelEntryLeaseByUID(IReplicationInContext context, String typeName, String uid, boolean isTransient, int routingValue) {
        if (_isPrimaryBackupSpace && ignoreOperation(isTransient))
            return;

        try {
            // this entry holder is safe to read from and will not be modified after this method returns
            IEntryHolder entryHolder = getLeaseManager().cancel(uid, typeName, ObjectTypes.ENTRY,
                    true /* fromRepl */, false /*origin*/, false /* isFromGateway */);

            if (context.getContentContext() != null) {
                if (entryHolder != null) {
                    IEntryData entryData = entryHolder.getEntryData();
                    context.getContentContext().setMainEntryData(entryData);
                } else if (context.getContextLogger().isLoggable(Level.FINE))
                    context.getContextLogger().log(Level.FINE, "Cancellation of entry lease did not return an associated entry: Type name:" + typeName +
                            " UID: " + uid);
            }


        } catch (UnknownLeaseException ex) {
            if (getConflictingOperationPolicy().isOverride())
                return;

            if (context.getContextLogger().isLoggable(Level.WARNING)) {
                context.getContextLogger().log(Level.WARNING, "Replicator: " + ex.getClass().getName() +
                        ". Failed to cancel Entry lease: " + typeName +
                        " UID: " + uid +
                        " ObjectType: " + ObjectTypes.ENTRY +
                        " in target [" + _engine.getFullSpaceName() + "] space."
                        + "\n" + ex.getMessage());
            }
        }
    }

    @Override
    public void inCancelEntryLease(IReplicationInContext context,
                                   IEntryPacket entryPacket) {
        inCancelEntryLeaseByUID(context,
                entryPacket.getTypeName(),
                entryPacket.getUID(),
                entryPacket.isTransient(),
                -1);
    }

    @Override
    public void inExtendEntryLeasePeriod(IReplicationInContext context, String typeName, String uid, boolean isTransient, long lease, int routingValue)
            throws UnknownLeaseException {
        if (_isPrimaryBackupSpace && ignoreOperation(isTransient))
            return;
        getLeaseManager().renew(uid, typeName, ObjectTypes.ENTRY,
                lease, true /* fromRepl */, false /*origin*/, false /* isFromGateway */);
    }

    @Override
    public void inEntryLeaseExpiredByUID(IReplicationInContext context, String typeName, String uid, boolean isTransient, OperationID operationID) {
        if (_isPrimaryBackupSpace && ignoreOperation(isTransient))
            return;

        try {
            // this entry holder is safe to read from and will not be modified after this method returns
            IEntryHolder entryHolder = getLeaseManager().cancel(uid, typeName, ObjectTypes.ENTRY,
                    true /* fromRepl */, false /*origin*/, true /*leaseExpired*/, operationID, false /* isFromReplication */);

            if (context.getContentContext() != null) {
                if (entryHolder != null) {
                    IEntryData entryData = entryHolder.getEntryData();
                    context.getContentContext().setMainEntryData(entryData);
                } else if (context.getContextLogger().isLoggable(Level.FINE))
                    context.getContextLogger().log(Level.FINE, "Expiration of entry lease did not return an associated entry: Type name:" + typeName +
                            " UID: " + uid);
            }

        } catch (UnknownLeaseException ex) {
            if (getConflictingOperationPolicy().isOverride())
                return;

            LogLevel logLevel = getLeaseManager().isSlaveLeaseManagerForEntries() ? LogLevel.DEBUG : LogLevel.WARNING;

            if (logLevel.isEnabled(context.getContextLogger())) {
                logLevel.log(context.getContextLogger(), "Replicator: " + ex.getClass().getName() +
                        ". Failed to expire Entry lease: " + typeName
                        + " UID: " + uid
                        + " ObjectType: " + ObjectTypes.ENTRY
                        + " in target [" + _engine.getFullSpaceName() + "] space."
                        + "\n" + ex.getMessage());
            }
        }
    }

    @Override
    public void inEntryLeaseExpired(IReplicationInContext context,
                                    IEntryPacket entryPacket) {
        inEntryLeaseExpiredByUID(context,
                entryPacket.getTypeName(),
                entryPacket.getUID(),
                entryPacket.isTransient(),
                entryPacket.getOperationID());
    }


}
