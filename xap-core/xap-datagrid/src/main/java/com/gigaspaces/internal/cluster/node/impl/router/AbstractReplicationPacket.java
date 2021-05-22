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

import com.gigaspaces.internal.cluster.node.impl.IIncomingReplicationFacade;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.utils.Textualizable;
import com.gigaspaces.internal.utils.Textualizer;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.lrmi.LRMIInvocationContext;
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public abstract class AbstractReplicationPacket<T> implements SmartExternalizable, Textualizable {
    private static final long serialVersionUID = 1L;
    private ReplicationEndpointDetails _sourceEndpointDetails;

    public AbstractReplicationPacket() {
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        PlatformLogicalVersion endpointLogicalVersion = LRMIInvocationContext.getEndpointLogicalVersion();
        IOUtils.writeRepetitiveObject(out, _sourceEndpointDetails);
        writeExternalImpl(out, endpointLogicalVersion);
    }

    protected abstract void writeExternalImpl(ObjectOutput out, PlatformLogicalVersion endpointLogicalVersion) throws IOException;

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        PlatformLogicalVersion endpointLogicalVersion = LRMIInvocationContext.getEndpointLogicalVersion();
        _sourceEndpointDetails = IOUtils.readRepetitiveObject(in);
        readExternalImpl(in, endpointLogicalVersion);
    }

    protected abstract void readExternalImpl(ObjectInput in, PlatformLogicalVersion endpointLogicalVersion) throws IOException, ClassNotFoundException;

    public void setSourceEndpointDetails(ReplicationEndpointDetails sourceEndpointDetails) {
        _sourceEndpointDetails = sourceEndpointDetails;
    }

    public ReplicationEndpointDetails getSourceEndpointDetails() {
        return _sourceEndpointDetails;
    }

    public String getSourceLookupName() {
        return _sourceEndpointDetails.getLookupName();
    }

    public Object getSourceUniqueId() {
        return _sourceEndpointDetails.getUniqueId();
    }

    /**
     * Process the replication packet at the target side.
     *
     * @param sourceMemberLookupName source of the packet
     * @param replicationNode        target replication node
     * @return process result
     */
    public abstract T accept(IIncomingReplicationFacade incomingReplicationFacade);

    @Override
    public String toString() {
        return Textualizer.toString(this);
    }

    @Override
    public void toText(Textualizer textualizer) {
        textualizer.appendString(toIdString());
        textualizer.appendString(": ");
        textualizer.append("sourceEndpointDetails", _sourceEndpointDetails);
    }

    public String toIdString() {
        return "@" + Integer.toHexString(hashCode());
    }
}
