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

package com.gigaspaces.internal.cluster.node.impl.router;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.utils.Textualizable;
import com.gigaspaces.internal.utils.Textualizer;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.lrmi.LRMIInvocationContext;
import com.gigaspaces.management.transport.ConnectionEndpointDetails;
import com.gigaspaces.serialization.SmartExternalizable;
import com.gigaspaces.start.SystemInfo;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Contains replication endpoint details
 *
 * @author eitany
 * @since 9.0.1
 */
@com.gigaspaces.api.InternalApi
public class ReplicationEndpointDetails implements SmartExternalizable, Textualizable {
    private static final long serialVersionUID = 1L;

    private String _lookupName;
    private Object _uniqueId;
    private String _hostname;
    private ConnectionEndpointDetails _connectionDetails;

    public static ReplicationEndpointDetails createMyEndpointDetails(String lookupName, Object uniqueId) {
        final String hostname = SystemInfo.singleton().network().getHost().getHostName();
        return new ReplicationEndpointDetails(lookupName, uniqueId, hostname, ConnectionEndpointDetails.create());
    }

    public ReplicationEndpointDetails cloneWithCurrentVersion() {
        return new ReplicationEndpointDetails(_lookupName, _uniqueId, _hostname, _connectionDetails.createCopy(PlatformLogicalVersion.getLogicalVersion()));
    }

    /**
     * @deprecated Should only be used for backward conversion details prior of 9.0.1
     */
    @Deprecated
    public static ReplicationEndpointDetails createBackwardEndpointDetails(String lookupName, Object uniqueId) {
        return new ReplicationEndpointDetails(lookupName, uniqueId, null, ConnectionEndpointDetails.EMPTY);
    }

    public ReplicationEndpointDetails() {
    }

    private ReplicationEndpointDetails(String lookupName, Object uniqueId, String hostname,
                                       ConnectionEndpointDetails connectionDetails) {
        _lookupName = lookupName;
        _uniqueId = uniqueId;
        _hostname = hostname;
        _connectionDetails = connectionDetails;
    }

    public Object getUniqueId() {
        return _uniqueId;
    }

    public String getLookupName() {
        return _lookupName;
    }

    public ConnectionEndpointDetails getConnectionDetails() {
        return _connectionDetails;
    }

    public String getHostname() {
        return _hostname;
    }

    public long getPid() {
        return _connectionDetails.getProcessId();
    }

    /**
     * This can return null if the endpoint which generated this is older than 9.0.1 because this
     * code is from 9.0.1 any older version will convert to a semi complete
     * ReplicationEndpointDetails
     */
    public PlatformLogicalVersion getPlatformLogicalVersion() {
        return _connectionDetails.getVersion();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((_uniqueId == null) ? 0 : _uniqueId.hashCode());
        return result;
    }

    //We want a cheap equals as this is used in writeRepetitiveObject, unique ID should suffice for equal purposes
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        ReplicationEndpointDetails other = (ReplicationEndpointDetails) obj;
        if (_uniqueId == null) {
            if (other._uniqueId != null)
                return false;
        } else if (!_uniqueId.equals(other._uniqueId))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return Textualizer.toString(this);
    }

    @Override
    public void toText(Textualizer textualizer) {
        textualizer.append("lookupName", getLookupName());
        textualizer.append("uniqueId", getUniqueId());
        textualizer.append("hostname", getHostname());
        textualizer.append("pid", getPid());
        textualizer.append("version", getPlatformLogicalVersion());
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        final PlatformLogicalVersion version = LRMIInvocationContext.getEndpointLogicalVersion();
        if (version.greaterOrEquals(PlatformLogicalVersion.v9_5_0))
            writeExternalV9_5_0(out);
        else
            writeExternalOld(out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        final PlatformLogicalVersion version = LRMIInvocationContext.getEndpointLogicalVersion();
        if (version.greaterOrEquals(PlatformLogicalVersion.v9_5_0))
            readExternalV9_5_0(in);
        else
            readExternalOld(in);
    }

    private void writeExternalV9_5_0(ObjectOutput out) throws IOException {
        IOUtils.writeRepetitiveString(out, _lookupName);
        IOUtils.writeRepetitiveObject(out, _uniqueId);
        IOUtils.writeRepetitiveString(out, _hostname);
        IOUtils.writeObject(out, _connectionDetails);
    }

    private void readExternalV9_5_0(ObjectInput in) throws IOException, ClassNotFoundException {
        _lookupName = IOUtils.readRepetitiveString(in);
        _uniqueId = IOUtils.readRepetitiveObject(in);
        _hostname = IOUtils.readRepetitiveString(in);
        _connectionDetails = IOUtils.readObject(in);
    }

    private void writeExternalOld(ObjectOutput out) throws IOException {
        IOUtils.writeRepetitiveString(out, _lookupName);
        IOUtils.writeRepetitiveObject(out, _uniqueId);
        out.writeLong(_connectionDetails.getProcessId());
        IOUtils.writeRepetitiveString(out, _hostname);
        IOUtils.writeRepetitiveObject(out, _connectionDetails.getVersion());
    }

    private void readExternalOld(ObjectInput in) throws IOException, ClassNotFoundException {
        _lookupName = IOUtils.readRepetitiveString(in);
        _uniqueId = IOUtils.readRepetitiveObject(in);
        long pid = in.readLong();
        _hostname = IOUtils.readRepetitiveString(in);
        PlatformLogicalVersion version = IOUtils.readRepetitiveObject(in);
        _connectionDetails = new ConnectionEndpointDetails(_hostname, null, pid, version);
    }
}
