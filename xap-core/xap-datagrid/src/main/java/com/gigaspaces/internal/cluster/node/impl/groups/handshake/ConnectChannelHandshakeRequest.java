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

import com.gigaspaces.internal.cluster.node.impl.backlog.IBacklogHandshakeRequest;
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
public class ConnectChannelHandshakeRequest implements SmartExternalizable {
    private static final long serialVersionUID = 1L;

    private IBacklogHandshakeRequest _backlogHandshakeRequest;

    public ConnectChannelHandshakeRequest() {
    }

    public ConnectChannelHandshakeRequest(IBacklogHandshakeRequest backlogHandshakeRequest) {
        _backlogHandshakeRequest = backlogHandshakeRequest;
    }

    public IBacklogHandshakeRequest getBacklogHandshakeRequest() {
        return _backlogHandshakeRequest;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeObject(out, _backlogHandshakeRequest);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        _backlogHandshakeRequest = IOUtils.readObject(in);
    }

    @Override
    public String toString() {
        return "ConnectChannelHandshakeRequest [getBacklogHandshakeRequest()=" + getBacklogHandshakeRequest() + "]";
    }


}
