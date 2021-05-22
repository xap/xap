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
package com.sun.jini.mahalo;

import com.gigaspaces.client.DirectSpaceProxyFactory;
import com.gigaspaces.internal.client.spaceproxy.IDirectSpaceProxy;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.lrmi.LRMIInvocationContext;
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * prepare result returned when clusterd proxy is needed
 *
 * @author yechielf
 */
@com.gigaspaces.api.InternalApi
public class ExtendedPrepareResult implements SmartExternalizable {
    private static final long serialVersionUID = 1L;

    private int _vote;
    private IDirectSpaceProxy _clusterdProxy; //needed for F.O. if not rendered in embedded join

    public ExtendedPrepareResult(int vote, IDirectSpaceProxy clusterdProxy) {
        _vote = vote;
        _clusterdProxy = clusterdProxy;
    }

    public ExtendedPrepareResult() {

    }

    public int getVote() {
        return _vote;
    }

    public IDirectSpaceProxy getProxy() {
        return _clusterdProxy;
    }

    private static final short FLAGS_PROXY = 1 << 0;

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        short flags = 0;
        if (_clusterdProxy != null)
            flags |= FLAGS_PROXY;
        out.writeShort(flags);
        out.writeInt(_vote);
        if (_clusterdProxy != null) {
            if (LRMIInvocationContext.getEndpointLogicalVersion().greaterOrEquals(PlatformLogicalVersion.v11_0_0))
                out.writeObject(_clusterdProxy.getFactory());
            else
                out.writeObject(_clusterdProxy);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        final short flags = in.readShort();
        _vote = in.readInt();
        if ((flags & FLAGS_PROXY) != 0) {
            if (LRMIInvocationContext.getEndpointLogicalVersion().greaterOrEquals(PlatformLogicalVersion.v11_0_0)) {
                DirectSpaceProxyFactory factory = (DirectSpaceProxyFactory) in.readObject();
                _clusterdProxy = factory.createSpaceProxy().getDirectProxy();
            } else {
                _clusterdProxy = (IDirectSpaceProxy) in.readObject();
            }
        }
    }
}
