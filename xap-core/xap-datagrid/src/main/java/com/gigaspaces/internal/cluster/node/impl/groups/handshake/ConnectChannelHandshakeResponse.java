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

package com.gigaspaces.internal.cluster.node.impl.groups.handshake;

import com.gigaspaces.internal.cluster.node.impl.processlog.IProcessLogHandshakeResponse;
import com.gigaspaces.internal.cluster.node.impl.router.ReplicationEndpointDetails;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author eitany
 * @since 9.0.1
 */
@com.gigaspaces.api.InternalApi
public class ConnectChannelHandshakeResponse
        implements SmartExternalizable {
    private static final long serialVersionUID = 1L;

    private ReplicationEndpointDetails _targetEndpointDetails;
    private IProcessLogHandshakeResponse _processLogHandshakeResponse;

    public ConnectChannelHandshakeResponse() {
    }

    public ConnectChannelHandshakeResponse(ReplicationEndpointDetails targetEndpointDetails,
                                           IProcessLogHandshakeResponse processLogHandshakeResponse) {
        _targetEndpointDetails = targetEndpointDetails;
        _processLogHandshakeResponse = processLogHandshakeResponse;
    }

    public ReplicationEndpointDetails getTargetEndpointDetails() {
        return _targetEndpointDetails;
    }

    public IProcessLogHandshakeResponse getProcessLogHandshakeResponse() {
        return _processLogHandshakeResponse;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeObject(out, _processLogHandshakeResponse);
        _targetEndpointDetails.writeExternal(out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        _processLogHandshakeResponse = IOUtils.readObject(in);
        _targetEndpointDetails = new ReplicationEndpointDetails();
        _targetEndpointDetails.readExternal(in);
    }

}
