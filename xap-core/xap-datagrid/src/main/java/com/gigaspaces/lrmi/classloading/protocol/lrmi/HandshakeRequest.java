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

package com.gigaspaces.lrmi.classloading.protocol.lrmi;

import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.internal.version.PlatformVersion;
import com.gigaspaces.serialization.SmartExternalizable;
import com.gigaspaces.start.SystemInfo;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.rmi.UnmarshalException;

/**
 * An LRMI handshake object
 *
 * @author eitany
 * @since 7.1
 */
@com.gigaspaces.api.InternalApi
public class HandshakeRequest implements SmartExternalizable {
    // DO NOT CHANGE. use SERIAL_VERSION instead.
    private static final long serialVersionUID = 1L;
    private static final byte SERIAL_VERSION = Byte.MIN_VALUE;

    private PlatformLogicalVersion _logicalVersion;
    private long _pid;

    //For Externalizable
    public HandshakeRequest() {

    }

    public HandshakeRequest(PlatformLogicalVersion logicalVersion) {
        _logicalVersion = logicalVersion;
        _pid = SystemInfo.singleton().os().processId();
    }

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        byte serialVersion = in.readByte();
        if (serialVersion != SERIAL_VERSION)
            throw new UnmarshalException("Requested version [" + serialVersion + "] does not match local version [" + SERIAL_VERSION + "]. Please make sure you are using the same version on both ends, service version is " + PlatformVersion.getOfficialVersion());
        _logicalVersion = (PlatformLogicalVersion) in.readObject();
        //Because handshake request is the last element read from the stream, on older version they should
        //be able to read the stream successfully and have a remaining data on the end of it which does not
        //interfere since nothing else is being read from stream afterwards
        if (_logicalVersion.greaterOrEquals(PlatformLogicalVersion.v9_1_0))
            _pid = in.readLong();
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeByte(SERIAL_VERSION);
        out.writeObject(_logicalVersion);
        out.writeLong(_pid);
    }

    public PlatformLogicalVersion getSourcePlatformLogicalVersion() {
        return _logicalVersion;
    }

    public long getSourcePid() {
        return _pid;
    }

}
