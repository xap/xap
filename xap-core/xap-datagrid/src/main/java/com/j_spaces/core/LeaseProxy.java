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

package com.j_spaces.core;

import com.gigaspaces.internal.lease.LeaseUtils;
import com.gigaspaces.internal.server.space.IRemoteSpace;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.serialization.SmartExternalizable;
import com.gigaspaces.time.SystemTime;

import net.jini.core.lease.Lease;
import net.jini.core.lease.LeaseDeniedException;
import net.jini.core.lease.LeaseMap;
import net.jini.core.lease.UnknownLeaseException;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.rmi.RemoteException;

/**
 * An instance of this class represents an entry/template lease or a lease of an event registration
 * results from an event registration. <br> You can get entry UID from the entry Lease object. e.g.
 *
 * <pre>
 * <code>
 * Message o = new Message ("A");
 * Lease lease = space.write(o, null, Lease.FOREVER);
 * LeaseProxy lp = (LeaseProxy) lease;
 * String uid = lp.getUID();
 * </code>
 * </pre>
 *
 * @author Igor Goldenberg
 * @version 1.0
 **/
@com.gigaspaces.api.InternalApi
public class LeaseProxy implements LeaseContext, SmartExternalizable {
    private static final long serialVersionUID = 1L;

    // net.jini.core.lease.Lease members:
    protected long _expirationTime;
    private int _serialFormat;

    // com.j_spaces.core.LeaseContext members
    private Object _previousObject;
    private String _uid;
    private int _version;

    // Implementation-specific members
    private String _classname; // set only in persistent spaces
    private int _objectType; // set only in persistent spaces
    private boolean _partialSerialization;

    private IRemoteSpace _remoteSpace;
    // If _embeddedSpace == null it means that Client works remote, else embedded.
    private transient IRemoteSpace _embeddedSpace;


    /**
     * Required for {@link java.io.Externalizable}.
     */
    public LeaseProxy() {
    }

    public LeaseProxy(long expirationTime, String uid, String classname, int version, int objectType) {
        _expirationTime = expirationTime;
        _serialFormat = Lease.DURATION;
        _uid = uid;
        _classname = classname;
        _version = version;
        _objectType = objectType;
    }

    public LeaseProxy(long expirationTime, String uid, String classname, int version, int objectType, SpaceImpl spaceImpl) {
        this(expirationTime, uid, classname, version, objectType);
        _embeddedSpace = spaceImpl;

        /**
         * Only if memory space and LeaseProxy of write operation, serialization
         * will be partially. we have to check every variable for
         * <code>null</code>, because on MemeryRecovery stage the Engine still
         * null.
         **/
        if (spaceImpl != null && spaceImpl.getEngine() != null
                && (spaceImpl.getEngine().getCacheManager().isMemorySpace() || spaceImpl.getEngine().getCacheManager().isBlobStoreCachePolicy())
                && objectType == ObjectTypes.ENTRY)
            _partialSerialization = true;
    }

    protected LeaseProxy(LeaseProxy lease) {
        this._expirationTime = lease._expirationTime;
        this._serialFormat = lease._serialFormat;
        this._partialSerialization = lease._partialSerialization;
        this._uid = lease._uid;
        this._classname = lease._classname;
        this._objectType = lease._objectType;
        this._previousObject = lease._previousObject;
    }

    @Override
    public long getExpiration() {
        return _expirationTime;
    }

    public void setExpiration(long exprTime) {
        _expirationTime = exprTime;
    }

    @Override
    public int getSerialFormat() {
        return _serialFormat;
    }

    @Override
    public void setSerialFormat(int format) {
        if (format != Lease.DURATION && format != Lease.ABSOLUTE)
            throw new IllegalArgumentException("Invalid serial format");

        _serialFormat = format;
    }

    @Override
    public void cancel()
            throws UnknownLeaseException, RemoteException {
        getSpace().cancel(_uid, _classname, _objectType);
    }

    @Override
    public void renew(long duration)
            throws LeaseDeniedException, UnknownLeaseException, RemoteException {
        final long newDuration = getSpace().renew(_uid, _classname, _objectType, duration);
        _expirationTime = LeaseUtils.toExpiration(newDuration);
    }

    @Override
    public boolean canBatch(Lease lease) {
        if (!(lease instanceof LeaseProxy))
            return false;

        LeaseProxy other = (LeaseProxy) lease;

        if (_embeddedSpace == null)
            return _remoteSpace.equals(other._remoteSpace);

        return _embeddedSpace.equals(other._embeddedSpace);
    }

    @Override
    public LeaseMap createLeaseMap(long duration) {
        return new LeaseMapProxy(this, duration);
    }

    @Override
    public String getUID() {
        return _uid;
    }

    public void setUID(String uid) {
        this._uid = uid;
    }

    @Override
    public int getVersion() {
        return _version;
    }

    @Override
    public Object getObject() {
        return _previousObject;
    }

    public void setObject(Object obj) {
        _previousObject = obj;
    }

    public int getObjectType() {
        return _objectType;
    }

    /**
     * Returns a class name of lease resource( Entry/Notify Template ). Only if and only if the
     * space is persistent.
     *
     * @return Returns class name of lease resource( Entry/Notify Template ).
     **/
    public String getClassname() {
        return _classname;
    }

    public IRemoteSpace getSpace() {
        return (_embeddedSpace == null) ? _remoteSpace : _embeddedSpace;
    }

    public void setRemoteSpace(IRemoteSpace remoteSpace) {
        _remoteSpace = remoteSpace;
    }

    @Override
    public void writeExternal(ObjectOutput out)
            throws IOException {
        /**
         * will be null, when server send to client, if this lease will be
         * serialize. by client, so we need be consistency.
         **/
        out.writeObject(_remoteSpace);

        out.writeUTF(_uid);
        out.writeInt(_serialFormat);

        // only if memory and not Notify Lease
        if (_partialSerialization)
            out.writeBoolean(true);
        else {
            // not partial serialization
            out.writeBoolean(false);
            if (_classname != null) {
                // classname not null
                out.writeBoolean(true);
                out.writeUTF(_classname);
            } else
                out.writeBoolean(false);
        }

        out.writeInt(_objectType);
        out.writeLong(_serialFormat == Lease.DURATION ? _expirationTime - SystemTime.timeMillis() : _expirationTime);

        out.writeObject(_previousObject);
        out.writeInt(_version);
    }

    @Override
    public void readExternal(ObjectInput in)
            throws IOException, ClassNotFoundException {
        /**
         * will be null, when server send to client, if this lease will be
         * serialize. by client, so we need be consistency.
         **/
        _remoteSpace = (IRemoteSpace) in.readObject();

        _uid = in.readUTF();
        _serialFormat = in.readInt();

        // is partial serialization
        _partialSerialization = in.readBoolean();

        /**
         * if not partial, usually this happen if lease is of persistent space
         * or this Notify lease.
         **/
        if (!_partialSerialization) {
            // is class not null
            if (in.readBoolean())
                _classname = in.readUTF();
        }

        _objectType = in.readInt();
        _expirationTime = in.readLong();
        if (_serialFormat == Lease.DURATION)
            _expirationTime = LeaseUtils.toExpiration(_expirationTime);

        _previousObject = in.readObject();
        _version = in.readInt();
    }
}
