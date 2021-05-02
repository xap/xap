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

package com.gigaspaces.internal.cluster.node.impl.backlog.globalorder;

import com.gigaspaces.internal.cluster.node.impl.backlog.IBacklogHandshakeRequest;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.lrmi.LRMIInvocationContext;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

@com.gigaspaces.api.InternalApi
public class GlobalOrderBacklogHandshakeRequest
        implements IBacklogHandshakeRequest {
    private static final long serialVersionUID = 1L;
    private boolean _firstHandshake;
    private long _lastConfirmedKey;
    private boolean resetTarget;

    public GlobalOrderBacklogHandshakeRequest() {
    }

    public GlobalOrderBacklogHandshakeRequest(boolean firstHandshake, long lastConfirmedKey) {
        _firstHandshake = firstHandshake;
        _lastConfirmedKey = lastConfirmedKey;
    }

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        _firstHandshake = in.readBoolean();
        _lastConfirmedKey = in.readLong();
        if(LRMIInvocationContext.getEndpointLogicalVersion().greaterOrEquals(PlatformLogicalVersion.v16_0_0)){
            this.resetTarget = in.readBoolean();
        }
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeBoolean(_firstHandshake);
        out.writeLong(_lastConfirmedKey);
        if(LRMIInvocationContext.getEndpointLogicalVersion().greaterOrEquals(PlatformLogicalVersion.v16_0_0)){
            out.writeBoolean(resetTarget);
        }
    }

    public long getLastConfirmedKey() {
        return _lastConfirmedKey;
    }

    public boolean isFirstHandshake() {
        return _firstHandshake;
    }

    @Override
    public boolean isResetTarget() {
        return resetTarget;
    }

    @Override
    public void setResetTarget(boolean resetTarget) {
        this.resetTarget = resetTarget;
    }

    public String toLogMessage() {
        return "Handshake request. First handshake by source [" + _firstHandshake + "]. Last confirmed key by source [" + _lastConfirmedKey + "]";
    }

    @Override
    public String toString() {
        return toLogMessage();
    }


}
