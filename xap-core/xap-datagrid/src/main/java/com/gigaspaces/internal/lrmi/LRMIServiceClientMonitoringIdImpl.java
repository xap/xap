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

package com.gigaspaces.internal.lrmi;

import com.gigaspaces.lrmi.LRMIServiceClientMonitoringId;
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.net.InetAddress;

/**
 * Tracking id of a single client on a specific service
 *
 * @author eitany
 * @see LRMIServiceClientMonitoringDetailsImpl
 * @see LRMIServiceMonitoringDetailsImpl
 * @since 9.1
 */
@com.gigaspaces.api.InternalApi
public class LRMIServiceClientMonitoringIdImpl implements SmartExternalizable, LRMIServiceClientMonitoringId {

    private static final long serialVersionUID = 1L;

    private InetAddress _remoteInetAddress;
    private long _sourcePid;

    public LRMIServiceClientMonitoringIdImpl() {
    }

    public LRMIServiceClientMonitoringIdImpl(InetAddress remoteInetAddress, long sourcePid) {
        _remoteInetAddress = remoteInetAddress;
        _sourcePid = sourcePid;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(_remoteInetAddress);
        out.writeLong(_sourcePid);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        _remoteInetAddress = (InetAddress) in.readObject();
        _sourcePid = in.readLong();
    }

    /* (non-Javadoc)
     * @see com.gigaspaces.lrmi.nio.LRMIServiceClientMonitoringId#getRemoteInetAddress()
	 */
    @Override
    public InetAddress getRemoteInetAddress() {
        return _remoteInetAddress;
    }

    /* (non-Javadoc)
     * @see com.gigaspaces.lrmi.nio.LRMIServiceClientMonitoringId#getSourcePid()
	 */
    @Override
    public long getSourcePid() {
        return _sourcePid;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((_remoteInetAddress == null) ? 0 : _remoteInetAddress.hashCode());
        result = prime * result + (int) (_sourcePid ^ (_sourcePid >>> 32));
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
        LRMIServiceClientMonitoringIdImpl other = (LRMIServiceClientMonitoringIdImpl) obj;
        if (_remoteInetAddress == null) {
            if (other._remoteInetAddress != null)
                return false;
        } else if (!_remoteInetAddress.equals(other._remoteInetAddress))
            return false;
        if (_sourcePid != other._sourcePid)
            return false;
        return true;
    }

    @Override
    public String toString() {
        return _remoteInetAddress.toString() + "[pid:" + _sourcePid + "]";
    }


}
