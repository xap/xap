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

package com.gigaspaces.management.transport;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.utils.Textualizable;
import com.gigaspaces.internal.utils.Textualizer;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.serialization.SmartExternalizable;
import com.gigaspaces.start.SystemInfo;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Encapsulates details about a connection end point.
 *
 * @author Niv Ingberg
 * @since 9.5.0
 */
@com.gigaspaces.api.InternalApi
public class ConnectionEndpointDetails implements SmartExternalizable, Textualizable {
    private static final long serialVersionUID = 1L;

    public static final ConnectionEndpointDetails EMPTY = new ConnectionEndpointDetails(null, null, -1, null);

    private String _hostName;
    private String _hostAddress;
    private long _processId;
    private PlatformLogicalVersion _version;

    public static ConnectionEndpointDetails create() {
        final String hostname = SystemInfo.singleton().network().getHost().getHostName();
        final String hostAddress = SystemInfo.singleton().network().getHost().getHostAddress();
        final long pid = SystemInfo.singleton().os().processId();
        final PlatformLogicalVersion version = PlatformLogicalVersion.getLogicalVersion();
        return new ConnectionEndpointDetails(hostname, hostAddress, pid, version);
    }

    /**
     * Required for Externalizable
     */
    public ConnectionEndpointDetails() {
    }

    public ConnectionEndpointDetails(String hostname, String hostAddress, long processId, PlatformLogicalVersion version) {
        this._hostName = hostname;
        this._hostAddress = hostAddress;
        this._processId = processId;
        this._version = version;
    }

    public ConnectionEndpointDetails createCopy(PlatformLogicalVersion newVersion) {
        return new ConnectionEndpointDetails(this._hostName, this._hostAddress, this._processId, newVersion);
    }

    @Override
    public String toString() {
        return Textualizer.toString(this);
    }

    @Override
    public void toText(Textualizer textualizer) {
        textualizer.append("hostName", getHostName());
        textualizer.append("hostAddress", getHostAddress());
        textualizer.append("pid", getProcessId());
        textualizer.append("version", getVersion());
    }

    public String getHostName() {
        return _hostName;
    }

    public String getHostAddress() {
        return _hostAddress;
    }

    public long getProcessId() {
        return _processId;
    }

    public PlatformLogicalVersion getVersion() {
        return _version;
    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((_hostAddress == null) ? 0 : _hostAddress.hashCode());
        result = prime * result
                + ((_hostName == null) ? 0 : _hostName.hashCode());
        result = prime * result + (int) (_processId ^ (_processId >>> 32));
        result = prime * result
                + ((_version == null) ? 0 : _version.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        ConnectionEndpointDetails other = (ConnectionEndpointDetails) obj;
        if (_hostAddress == null) {
            if (other._hostAddress != null)
                return false;
        } else if (!_hostAddress.equals(other._hostAddress))
            return false;
        if (_hostName == null) {
            if (other._hostName != null)
                return false;
        } else if (!_hostName.equals(other._hostName))
            return false;
        if (_processId != other._processId)
            return false;
        if (_version == null) {
            if (other._version != null)
                return false;
        } else if (!_version.equals(other._version))
            return false;
        return true;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeString(out, _hostName);
        IOUtils.writeString(out, _hostAddress);
        out.writeLong(_processId);
        IOUtils.writeRepetitiveObject(out, _version);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this._hostName = IOUtils.readString(in);
        this._hostAddress = IOUtils.readString(in);
        this._processId = in.readLong();
        this._version = IOUtils.readRepetitiveObject(in);
    }
}
