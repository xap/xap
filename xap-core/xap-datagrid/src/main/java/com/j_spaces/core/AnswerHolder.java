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

package com.j_spaces.core;

import com.gigaspaces.admin.quiesce.QuiesceException;
import com.gigaspaces.client.protective.ProtectiveModeException;
import com.gigaspaces.events.GSEventRegistration;
import com.gigaspaces.internal.query.explainplan.SingleExplainPlan;
import com.gigaspaces.internal.server.space.operations.WriteEntriesResult;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.j_spaces.core.client.EntryNotInSpaceException;
import com.j_spaces.core.client.EntryVersionConflictException;
import com.j_spaces.kernel.JSpaceUtilities;

import net.jini.core.transaction.TransactionException;
import net.jini.space.InternalSpaceException;

/**
 * Instances of this class are mediators between the receiver to the rest of the engine. The
 * receiver creates an AnswerHolder instance and a Bus Packet with a reference to this instance. The
 * engine processes the Bus packet. When done, the engine notifies the receiver thread by calling
 * the <code>notify()</code> method on the AnswerHolder instance.
 */
@com.gigaspaces.api.InternalApi
public class AnswerHolder {
    private SingleExplainPlan _SingleExplainPlan;

    public AnswerHolder() {

    }

    public volatile AnswerPacket m_AnswerPacket = AnswerPacket.DummyPacket;

    private GSEventRegistration _eventRegistration;

    public volatile Exception m_Exception;

    // returned by read/take batch operations
    private IEntryPacket[] _entryPackets;

    // returned by updateMultiple/updateOrWriteMultiple
    private WriteEntriesResult _updateMultipleResult;

    public int _numOfEntriesMatched;

    private int syncRelplicationLevel;

    public int getSyncRelplicationLevel() {
        return syncRelplicationLevel;
    }

    public void setSyncRelplicationLevel(int syncRelplicationLevel) {
        this.syncRelplicationLevel = syncRelplicationLevel;
    }

    public void setEventRegistration(GSEventRegistration m_EventRegistration) {
        this._eventRegistration = m_EventRegistration;
    }

    public GSEventRegistration getEventRegistration() {
        return _eventRegistration;
    }

    public AnswerPacket getResult()
            throws EntryVersionConflictException, EntryNotInSpaceException, TransactionException {
        throwExceptionIfExists();
        return m_AnswerPacket;
    }

    public AnswerPacket getAnswerPacket() {
        return m_AnswerPacket;
    }

    public Exception getException() {
        return m_Exception;
    }

    public IEntryPacket[] getEntryPackets() {
        return _entryPackets;
    }

    public void setEntryPackets(IEntryPacket[] entryPackets) {
        _entryPackets = entryPackets;
    }

    public WriteEntriesResult getUpdateMultipleResult() {
        return _updateMultipleResult;
    }

    public void setUpdateMultipleResult(WriteEntriesResult updateMultipleResult) {
        _updateMultipleResult = updateMultipleResult;
    }

    public int getNumOfEntriesMatched() {
        return _numOfEntriesMatched;
    }

    public void setNumOfEntriesMatched(int numOfEntriesMatched) {
        _numOfEntriesMatched = numOfEntriesMatched;
    }

    public void throwExceptionIfExists()
            throws TransactionException, EntryVersionConflictException, EntryNotInSpaceException {
        Exception exception = m_Exception;
        if (exception != null) {
            if (exception instanceof TransactionException)
                throw (TransactionException) exception;
            if (exception instanceof InternalSpaceException)
                throw (InternalSpaceException) exception;
            if (exception instanceof EntryVersionConflictException)
                throw (EntryVersionConflictException) exception;
            if (exception instanceof EntryNotInSpaceException)
                throw (EntryNotInSpaceException) exception;
            if (exception instanceof ProtectiveModeException)
                throw (ProtectiveModeException) exception;
            if (exception instanceof IllegalArgumentException)
                throw (IllegalArgumentException) exception;
            if (exception instanceof QuiesceException)
                throw (QuiesceException)exception;

            JSpaceUtilities.throwEngineInternalSpaceException(exception.toString(), exception);
        }

    }

    public void setExplainPlan(SingleExplainPlan plan) {
        this._SingleExplainPlan = plan;
    }

    public SingleExplainPlan getExplainPlan() {
        return _SingleExplainPlan;
    }
}
