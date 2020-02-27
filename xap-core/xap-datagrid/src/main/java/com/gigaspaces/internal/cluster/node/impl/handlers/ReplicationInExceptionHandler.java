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
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.gigaspaces.logger.LogLevel;
import com.j_spaces.core.client.EntryNotInSpaceException;
import com.j_spaces.core.client.EntryVersionConflictException;
import com.j_spaces.core.cluster.ClusterXML;

import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * Handles exceptions on consuming replication packets.
 *
 * @author anna
 * @since 8.0
 */
@com.gigaspaces.api.InternalApi
public class ReplicationInExceptionHandler {
    private static final String WRITE = "Write";
    private static final String UPDATE = "Update";
    private static final String TAKE = "Take";
    private static final String CHANGE = "Change";

    protected String _spaceName;
    private final boolean _isCentralDB;

    public ReplicationInExceptionHandler(String spaceName, boolean isCentralDB) {
        _spaceName = spaceName;
        _isCentralDB = isCentralDB;
    }

    public void handleEntryLockedByTransactionOnTake(Logger logger, IEntryPacket entryPacket) {
        if (_isCentralDB && !entryPacket.isTransient())
            return;
        if (logger != null && logger.isLoggable(Level.SEVERE)) {
            logMessage(logger,
                    TAKE,
                    entryPacket.getTypeName(),
                    entryPacket.getUID(),
                    LogLevel.SEVERE,
                    "Entry is locked by another transaction.");
        }
    }

    public void handleEntryAlreadyInSpaceOnWrite(
            IReplicationInContext context, IEntryPacket entryPacket) {
        // duplicate entries in central db is allowed
        if (_isCentralDB && !entryPacket.isTransient())
            return;

        /** failed to consume */
        LogLevel level = getDefaultExceptionLevel();
        if (context.getContextLogger() != null && level.isEnabled(context.getContextLogger())) {
            logMessage(context.getContextLogger(),
                    WRITE,
                    entryPacket.getTypeName(),
                    entryPacket.getUID(),
                    level,
                    "Entry already in space");
        }
    }

    public void handleEntryNotInSpaceOnTake(Logger logger,
                                            IEntryPacket entryPacket) {
        LogLevel logLevel = getDefaultExceptionLevel();
        if (logger != null && logLevel.isEnabled(logger) && !(_isCentralDB && !entryPacket.isTransient())) {
            logMessage(logger,
                    TAKE,
                    entryPacket.getTypeName(),
                    entryPacket.getUID(),
                    logLevel,
                    "Entry not in space");

        } else {

            if (logger != null && logger.isLoggable(Level.FINE))
                logMessage(logger,
                        TAKE,
                        entryPacket.getTypeName(),
                        entryPacket.getUID(),
                        LogLevel.DEBUG,
                        "Entry not in space");

        }
    }

    public void handleEntryVersionConflictOnTake(Logger logger,
                                                 IEntryPacket entryPacket, EntryVersionConflictException ex) {
        LogLevel logLevel = getDefaultExceptionLevel();
        if (logger != null && logLevel.isEnabled(logger)) {
            logMessage(logger,
                    TAKE,
                    entryPacket.getTypeName(),
                    entryPacket.getUID(),
                    logLevel,
                    "Version conflict - in space entry version is <"
                            + ex.getSpaceVersionID()
                            + ">, replicated version <"
                            + ex.getClientVersionID() + ">.");

        }
    }


    public void handleNoClassNameOnTake(Logger logger,
                                        String uid) {
        if (logger != null && logger.isLoggable(Level.SEVERE)) {
            logger.severe("Replication detected illegal " + TAKE + " operation on entry "
                    + " uid=<"
                    + uid
                    + ">\n"
                    + "  Symptom: Entry class name wasn't replicated.\n"
                    + "  Ignoring the illegal operation \n"
                    + "  Please make sure that property <"
                    + ClusterXML.CLUSTER_CONFIG_TAG + "." + ClusterXML.CACHE_LOADER_TAG + "." + ClusterXML.CACHE_LOADER_EXTERNAL_DATA_SOURCE
                    + "> is set to 'true'.");
        }

    }

    public void handleEntryLockedByTransactionOnUpdate(Logger logger,
                                                       IEntryPacket entryPacket) {
        if (_isCentralDB && !entryPacket.isTransient())
            return;
        if (logger != null && logger.isLoggable(Level.SEVERE)) {
            logMessage(logger, UPDATE, entryPacket.getTypeName(), entryPacket.getUID(), LogLevel.SEVERE, "Entry is locked by another transaction.");
        }
    }

    public void handleEntryLockedByTransactionOnChange(Logger logger,
                                                       ITemplatePacket entryPacket) {
        if (logger != null && logger.isLoggable(Level.SEVERE)) {
            logMessage(logger, CHANGE, entryPacket.getTypeName(), entryPacket.getUID(), LogLevel.SEVERE, "Entry is locked by another transaction.");
        }
    }

    public void handleEntryVersionConflictOnUpdate(Logger logger,
                                                   IEntryPacket entryPacket, EntryVersionConflictException ex) {
        handleEntryVersionConflictOnUpdate(logger,
                entryPacket, ex, getDefaultExceptionLevel());
    }

    public void handleEntryVersionConflictOnChange(Logger logger,
                                                   ITemplatePacket entryPacket, EntryVersionConflictException ex) {
        handleEntryVersionConflictOnChange(logger,
                entryPacket, ex, getDefaultExceptionLevel());
    }

    public void handleEntryVersionConflictOnUpdate(Logger logger,
                                                   IEntryPacket entryPacket, EntryVersionConflictException ex, LogLevel logLevel) {

        if (logger != null && logLevel.isEnabled(logger)) {
            logMessage(logger,
                    UPDATE,
                    entryPacket.getTypeName(),
                    entryPacket.getUID(),
                    logLevel,
                    "Version conflict - in space entry version is <"
                            + ex.getSpaceVersionID()
                            + ">, replicated version <"
                            + ex.getClientVersionID() + ">.");

        }
    }

    public void handleEntryVersionConflictOnChange(Logger logger,
                                                   ITemplatePacket entryPacket, EntryVersionConflictException ex,
                                                   LogLevel logLevel) {
        if (logger != null && logLevel.isEnabled(logger)) {
            logMessage(logger,
                    CHANGE,
                    entryPacket.getTypeName(),
                    entryPacket.getUID(),
                    logLevel,
                    "Version conflict - in space entry version is <"
                            + ex.getSpaceVersionID()
                            + ">, replicated version <"
                            + ex.getClientVersionID() + ">.");
        }

    }


    public void handleEntryNotInSpaceOnUpdate(Logger logger,
                                              IEntryPacket entryPacket, EntryNotInSpaceException ex) {

        LogLevel logLevel = getDefaultExceptionLevel();
        if (logger != null && logLevel.isEnabled(logger)) {
            logMessage(logger,
                    UPDATE,
                    entryPacket.getTypeName(),
                    entryPacket.getUID(),
                    logLevel,
                    "Entry is not in space");

        }

    }

    public void handleEntryNotInSpaceOnChange(Logger logger, ITemplatePacket entryPacket) {
        LogLevel logLevel = getDefaultExceptionLevel();
        if (logger != null && logLevel.isEnabled(logger)) {
            logMessage(logger,
                    CHANGE,
                    entryPacket.getTypeName(),
                    entryPacket.getUID(),
                    logLevel,
                    "Entry is not in space");
        }
    }


    protected LogLevel getDefaultExceptionLevel() {
        return LogLevel.SEVERE;
    }


    protected void logMessage(Logger logger, String operation,
                              String className, String uid, LogLevel logLevel, String symptom) {
        logLevel.log(logger,
                "Replication detected conflicting "
                        + operation
                        + " operation on entry - "
                        + "<"
                        + className + ">"
                        + " uid=<"
                        + uid
                        + ">\n"
                        + "  Symptom: "
                        + symptom
                        + "\n"
                        + "  Ignoring the conflicting operation since it has already been applied to space [" + _spaceName + "].\n"
                        + "  Please make sure that the entry was not simultaneously changed in two different space instances.");
    }

}

