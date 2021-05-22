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

import com.gigaspaces.internal.server.space.operations.WriteEntryResult;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * This internal class for communication between client/server protocol. Essentially this class
 * transfered as result from read/take operations from server to client.
 *
 * @author Igor Goldenberg
 * @version 1.0
 */
@com.gigaspaces.api.InternalApi
public class AnswerPacket implements SmartExternalizable {
    private static final long serialVersionUID = -5466254335883432404L;
    static final AnswerPacket DummyPacket = new AnswerPacket();
    static public final AnswerPacket NullPacket = new AnswerPacket();


    public IEntryPacket m_EntryPacket;

    /**
     * non null iff update-or-write resulted in a write
     */
    public LeaseContext m_leaseProxy;
    private transient WriteEntryResult _writeEntryResult;

    /**
     * default constructor for externelizable implementation.
     */
    public AnswerPacket() {
    }

    public AnswerPacket(IEntryPacket entryPacket) {
        m_EntryPacket = entryPacket;
    }

    public AnswerPacket(LeaseContext lease) {
        m_leaseProxy = lease;
    }

    public AnswerPacket(WriteEntryResult writeEntryResult) {
        _writeEntryResult = writeEntryResult;
    }

    public WriteEntryResult getWriteEntryResult() {
        return _writeEntryResult;
    }

    public AnswerPacket(IEntryPacket entryPacket, WriteEntryResult writeResult) {
        this.m_EntryPacket = entryPacket;
        if (writeResult != null) {
            this._writeEntryResult = writeResult;
            this._writeEntryResult.setPrevEntry(entryPacket);
        }
    }

    public void readExternal(ObjectInput in)
            throws IOException, ClassNotFoundException {
        // is EntryPacket != null
        if (in.readBoolean()) {
            m_EntryPacket = (IEntryPacket) in.readObject();
        }

        m_leaseProxy = (LeaseContext) in.readObject();

    }

    public void writeExternal(ObjectOutput out)
            throws IOException {
        if (m_EntryPacket != null) {
            // entryPacket not null
            out.writeBoolean(true);

            /**
             * More comfortable using existing stuff, use java.io.Externalizable in following way.
             * This allows you to only send/receive the content of object m_EntryPacket..
             **/
            out.writeObject(m_EntryPacket);
        } else // entryPacket null
            out.writeBoolean(false);


        out.writeObject(m_leaseProxy);
    }

    public boolean isDummy() {
        return this == DummyPacket;
    }

    public String toString() {
        return "[AnswerPacket entryPacket = " + m_EntryPacket + ", isDummy = " + isDummy() + "]";
    }
}