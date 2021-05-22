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
package com.sun.jini.proxy;

import com.gigaspaces.serialization.SmartExternalizable;
import com.sun.jini.start.ServiceProxyAccessor;

import java.io.IOException;
import java.io.NotActiveException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.ObjectStreamException;
import java.rmi.RemoteException;
import java.rmi.UnmarshalException;

/**
 * Default implementation for of {@link MarshalPivot} providing a proxy replacement on the LUS.
 *
 * @author Guy
 */
@com.gigaspaces.api.InternalApi
public class DefaultProxyPivot implements MarshalPivot, SmartExternalizable {

    private static final ThreadLocal<Boolean> LAZY_ACCESS = new ThreadLocal<Boolean>() {
        @Override
        protected Boolean initialValue() {
            return false;
        }
    };

    private static final long serialVersionUID = 1L;
    private static final byte SERIAL_VERSION = Byte.MIN_VALUE;

    private ServiceProxyAccessor service;

    public DefaultProxyPivot() {
    }

    public DefaultProxyPivot(ServiceProxyAccessor service) {
        this.service = service;
    }

    @Override
    public void writeExternal(ObjectOutput out)
            throws IOException {
        out.writeByte(SERIAL_VERSION);
        out.writeObject(service);
    }

    @Override
    public void readExternal(ObjectInput in)
            throws IOException, ClassNotFoundException {
        byte version = in.readByte();
        if (version != SERIAL_VERSION)
            throw new UnmarshalException("Requested version [" + version + "] does not match local version [" + SERIAL_VERSION + "].");

        service = (ServiceProxyAccessor) in.readObject();
    }

    @Override
    public Object readResolve()
            throws ObjectStreamException {
        try {
            return isLazyAccess() ? service : service.getServiceProxy();
        } catch (RemoteException e) {
            ObjectStreamException streamException = new NotActiveException("Failed to retrieve proxy from service, cause " + e.getMessage());
            streamException.initCause(e);
            throw streamException;
        }
    }

    public static boolean updateLazyAccess(boolean lazyAccess) {
        final boolean previousValue = isLazyAccess();
        LAZY_ACCESS.set(lazyAccess);
        return previousValue;
    }

    private static boolean isLazyAccess() {
        return LAZY_ACCESS.get();
    }
}
