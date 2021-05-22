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

/*
 * @(#)LeaseInitializer.java 1.0  Nov 23, 2004  1:24:44 AM
 */

package com.j_spaces.core;

import com.gigaspaces.internal.lease.SpaceEntryLease;
import com.gigaspaces.serialization.SmartExternalizable;
import com.j_spaces.core.client.EntryInfo;

import net.jini.core.lease.Lease;
import net.jini.core.lease.LeaseMap;
import net.jini.core.lease.UnknownLeaseException;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.rmi.RemoteException;

/**
 * This utility class initializes LeaseProxy, set Remote/Local Space Reference
 *
 * @author Igor Goldenberg
 * @version 4.0
 **/
@com.gigaspaces.api.InternalApi
public class LeaseInitializer {
    public static class UIDLease implements LeaseContext<Object>, SmartExternalizable {
        private static final long serialVersionUID = 1L;

        private String _uid;
        private int _version;

        /**
         * default no-args public constructor for Externalizable impl
         */
        public UIDLease() {
        }

        private UIDLease(String uid, int version) {
            this._uid = uid;
            this._version = version;
        }

        @Override
        public String getUID() {
            return _uid;
        }

        @Override
        public int getVersion() {
            return _version;
        }

        @Override
        public Object getObject() {
            return null;
        }

        @Override
        public long getExpiration() {
            return 0;
        }

        @Override
        public void writeExternal(ObjectOutput out)
                throws IOException {
            out.writeUTF(_uid);
            out.writeInt(_version);
        }

        @Override
        public void readExternal(ObjectInput in)
                throws IOException, ClassNotFoundException {
            _uid = in.readUTF();
            _version = in.readInt();
        }

        @Override
        public void cancel() throws UnknownLeaseException, RemoteException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void renew(long l) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setSerialFormat(int i) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getSerialFormat() {
            throw new UnsupportedOperationException();
        }

        @Override
        public LeaseMap createLeaseMap(long l) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean canBatch(Lease lease) {
            throw new UnsupportedOperationException();
        }
    }

    public static class UpdateContextLease extends UIDLease {
        private static final long serialVersionUID = 1L;

        /**
         * The previous value which was in the space before update
         */
        private Object previousValue;

        /**
         * default no-args public constructor for Externalizable impl
         */
        public UpdateContextLease() {
        }

        private UpdateContextLease(String uid, int version, Object prevValue) {
            super(uid, version);
            previousValue = prevValue;
        }

        @Override
        public void writeExternal(ObjectOutput out)
                throws IOException {
            super.writeExternal(out);
            out.writeObject(previousValue);
        }

        @Override
        public void readExternal(ObjectInput in)
                throws IOException, ClassNotFoundException {
            super.readExternal(in);
            previousValue = in.readObject();
        }

        @Override
        public Object getObject() {
            return previousValue;
        }

        private void setObject(Object prevObj) {
            previousValue = prevObj;
        }
    }

    public static void setPreviousObject(LeaseContext<?> lease, Object previousObject) {
        if (lease instanceof SpaceEntryLease) {
            ((SpaceEntryLease<Object>) lease).setObject(previousObject);
        } else if (lease instanceof LeaseProxy)
            ((LeaseProxy) lease).setObject(previousObject);
        else if (lease instanceof UpdateContextLease)
            ((UpdateContextLease) lease).setObject(previousObject);
        else
            throw new IllegalArgumentException();
    }

    public static LeaseContext<?> createDummyLease(String uid, int version) {
        return new UIDLease(uid, version);
    }

    public static LeaseContext<?> createDummyLease(String uid, int version, Object obj) {
        return new UpdateContextLease(uid, version, obj);
    }

    public static EntryInfo getEntryInfo(LeaseContext<?> lease) {
        return new EntryInfo(lease.getUID(), 1);
    }

    public static boolean isDummyLease(LeaseContext<?> lease) {
        return lease instanceof UIDLease;
    }
}
