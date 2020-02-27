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

import com.gigaspaces.events.NotifyInfo;
import com.gigaspaces.internal.cluster.node.IReplicationInContext;
import com.gigaspaces.internal.cluster.node.handlers.IReplicationInNotifyTemplateCreatedHandler;
import com.gigaspaces.internal.cluster.node.handlers.IReplicationInNotifyTemplateLeaseExpiredHandler;
import com.gigaspaces.internal.cluster.node.handlers.IReplicationInNotifyTemplateLeaseExtendedHandler;
import com.gigaspaces.internal.cluster.node.handlers.IReplicationInNotifyTemplateRemovedHandler;
import com.gigaspaces.internal.server.space.SpaceEngine;
import com.gigaspaces.internal.server.space.metadata.SpaceTypeManager;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.logger.LogLevel;
import com.j_spaces.core.LeaseManager;
import com.j_spaces.core.ObjectTypes;
import com.j_spaces.core.OperationID;
import com.j_spaces.core.cluster.ConflictingOperationPolicy;

import net.jini.core.lease.UnknownLeaseException;
import net.jini.space.InternalSpaceException;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Niv Ingberg
 * @since 8.0.5
 */
@com.gigaspaces.api.InternalApi
public class SpaceReplicationTemplateEventHandler implements
        IReplicationInNotifyTemplateCreatedHandler,
        IReplicationInNotifyTemplateRemovedHandler,
        IReplicationInNotifyTemplateLeaseExtendedHandler,
        IReplicationInNotifyTemplateLeaseExpiredHandler {
    private static final Logger _logger = Logger.getLogger(Constants.LOGGER_REPLICATION);

    private final SpaceEngine _engine;
    private final SpaceTypeManager _typeManager;

    public SpaceReplicationTemplateEventHandler(SpaceEngine engine) {
        this._engine = engine;
        this._typeManager = engine.getTypeManager();
    }

    // TODO: use final member instead of lazy getter when LeaseManager construction is refactored.
    private LeaseManager getLeaseManager() {
        return _engine.getLeaseManager();
    }

    // TODO: use final member instead of lazy getter when ConflictingOperationPolicy construction is refactored.
    private ConflictingOperationPolicy getConflictingOperationPolicy() {
        return _engine.getConflictingOperationPolicy();
    }

    @Override
    public void inInsertNotifyTemplate(IReplicationInContext context, ITemplatePacket template, String templateUid, NotifyInfo notifyInfo)
            throws Exception {
        _typeManager.loadServerTypeDesc(template);

        _engine.notify(template, template.getTTL(),
                true /* fromRepl */, templateUid, null, notifyInfo);
    }

    @Override
    public void inRemoveNotifyTemplate(IReplicationInContext context, String typeName, String uid) {
        try {
            getLeaseManager().cancel(uid, typeName, ObjectTypes.NOTIFY_TEMPLATE,
                    true /* fromRepl */, false /*origin*/, false /* isfromGateway */);
        } catch (UnknownLeaseException e) {
            if (getConflictingOperationPolicy().isOverride())
                return;

            if (_logger.isLoggable(Level.WARNING)) {
                _logger.log(Level.WARNING, "Replicator: " + e.getClass().getName() +
                        ". Failed to cancel NotifyTemplate lease: " + typeName +
                        " UID: " + uid +
                        " ObjectType: " + ObjectTypes.NOTIFY_TEMPLATE +
                        " in target [" + _engine.getFullSpaceName() + "] space."
                        + "\nThis NotifyTemplate might be already expired or canceled.");
            }
        }
    }

    @Override
    public void inExtendNotifyTemplateLeasePeriod(IReplicationInContext context, String typeName, String uid, long lease)
            throws UnknownLeaseException, InternalSpaceException {
        getLeaseManager().renew(uid, typeName, ObjectTypes.NOTIFY_TEMPLATE,
                lease, true /* fromRepl */, false /*origin*/, false /* isFromGateway */);
    }

    @Override
    public void inNotifyTemplateLeaseExpired(IReplicationInContext context, String className, String uid, OperationID operationID) {
        try {
            getLeaseManager().cancel(uid, className, ObjectTypes.NOTIFY_TEMPLATE,
                    true /* fromRepl */, false /*origin*/, true /*leaseExpired*/, operationID, false /* isFromGateway */);
        } catch (UnknownLeaseException ex) {
            if (getConflictingOperationPolicy().isOverride())
                return;

            LogLevel logLevel = getLeaseManager().isSlaveLeaseManagerForNotifyTemplates() ? LogLevel.DEBUG : LogLevel.WARNING;
            if (logLevel.isEnabled(_logger)) {
                logLevel.log(_logger, "Replicator: " + ex.getClass().getName() +
                        ". Failed to expire NotifyTemplate lease: " + className +
                        " UID: " + uid +
                        " ObjectType: " + ObjectTypes.NOTIFY_TEMPLATE +
                        " in target [" + _engine.getFullSpaceName() + "] space."
                        + "\nThis NotifyTemplate might be already canceled.");
            }
        }
    }
}
