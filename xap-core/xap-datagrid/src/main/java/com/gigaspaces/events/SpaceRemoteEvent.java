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


package com.gigaspaces.events;

import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.lrmi.LRMIInvocationContext;

import com.gigaspaces.serialization.SmartExternalizable;
import net.jini.core.event.RemoteEvent;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.rmi.MarshalledObject;

/**
 * Base class for space remote events.
 *
 * @author Niv Ingberg
 * @since 8.0.4
 */

public class SpaceRemoteEvent extends RemoteEvent implements SmartExternalizable {
    private static final long serialVersionUID = 1L;
    protected static final String EMPTY_STRING = "";

    /**
     * Required for Externalizable
     */
    public SpaceRemoteEvent() {
        super(EMPTY_STRING, 0, 0, null);
    }

    /**
     * Constructs a SpaceRemoteEvent object. <p> The abstract state contained in a RemoteEvent
     * object includes a reference to the object in which the event occurred, a long which
     * identifies the kind of event relative to the object in which the event occurred, a long which
     * indicates the sequence number of this instance of the event kind, and a MarshalledObject that
     * is to be handed back when the notification occurs. The combination of the event identifier
     * and the object reference obtained from the RemoteEvent object should uniquely identify the
     * event type.
     *
     * @param source   an <tt>Object</tt> representing the event source
     * @param eventID  a <tt>long</tt> containing the event identifier
     * @param seqNum   a <tt>long</tt> containing the event sequence number
     * @param handback a <tt>MarshalledObject</tt> that was passed in as part of the original event
     *                 registration.
     */
    public SpaceRemoteEvent(Object source, long eventID, long seqNum, MarshalledObject handback) {
        super(source, eventID, seqNum, handback);
    }

    /**
     * Required for Externalizable
     */
    @Override
    public void writeExternal(ObjectOutput out)
            throws IOException {
        PlatformLogicalVersion version = LRMIInvocationContext.getEndpointLogicalVersion();
        writeExternal(out, version);
    }

    /**
     * Reserved for internal usage.
     */
    protected void writeExternal(ObjectOutput out, PlatformLogicalVersion version)
            throws IOException {
    }

    /**
     * Required for Externalizable
     */
    @Override
    public void readExternal(ObjectInput in)
            throws IOException, ClassNotFoundException {
        PlatformLogicalVersion version = LRMIInvocationContext.getEndpointLogicalVersion();
        readExternal(in, version);
    }

    /**
     * Reserved for internal usage.
     */
    protected void readExternal(ObjectInput in, PlatformLogicalVersion version)
            throws IOException, ClassNotFoundException {
    }
}
