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

package com.gigaspaces.internal.client.spaceproxy;

import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.lrmi.LRMIInvocationContext;
import com.gigaspaces.serialization.SmartExternalizable;
import com.j_spaces.core.client.ProxySettings;
import com.j_spaces.core.service.Service;

import net.jini.id.ReferentUuid;
import net.jini.id.Uuid;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Title: Description:
 *
 * @author Shay Banon
 * @version 1.0
 * @Company: GigaSpaces Technologies
 * @since 5.2
 */
public abstract class AbstractDirectSpaceProxy extends AbstractSpaceProxy implements IDirectSpaceProxy, Service, ReferentUuid, SmartExternalizable {
    private static final long serialVersionUID = 1L;
    private static final int SERIAL_VERSION = 1;

    protected ProxySettings _proxySettings;

    public AbstractDirectSpaceProxy(ProxySettings proxySettings) {
        this._proxySettings = proxySettings;
    }

    @Override
    public ProxySettings getProxySettings() {
        return _proxySettings;
    }

    /**
     * provides the consistent cross VM proxy equals implementation by {@link Uuid}
     */
    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;

        if (!(obj instanceof AbstractDirectSpaceProxy))
            return false;

        AbstractDirectSpaceProxy other = (AbstractDirectSpaceProxy) obj;
        return this.getProxySettings().getUuid().equals(other.getProxySettings().getUuid()) &&
                this.isClustered() == other.isClustered();
    }

    @Override
    public int hashCode() {
        return getProxySettings().getUUIDHashCode();
    }

    @Override
    public String toString() {
        /* NOTE: Don't change! In many places we assume on toString() which returns the space member-name. */
        return getProxySettings().getMemberName();
    }

    private static final byte FLAG_PROXYSETTINGS = 1 << 0;

    private byte buildFlags() {
        byte flags = 0;
        if (_proxySettings != null)
            flags |= FLAG_PROXYSETTINGS;
        return flags;
    }

    @Override
    public void readExternal(ObjectInput in)
            throws IOException, ClassNotFoundException {
        throw new IllegalStateException("This class should no longer be serialized");
    }

    @Override
    public void writeExternal(ObjectOutput out)
            throws IOException {
        if (LRMIInvocationContext.getEndpointLogicalVersion().greaterOrEquals(PlatformLogicalVersion.v11_0_0))
            throw new IllegalStateException("Class " + this.getClass().getName() + " should no longer be serialized");

        out.writeInt(SERIAL_VERSION);

        final byte flags = buildFlags();
        out.writeByte(flags);

        if (flags == 0)
            return;

        if (_proxySettings != null)
            _proxySettings.writeExternal(out);
    }
}
