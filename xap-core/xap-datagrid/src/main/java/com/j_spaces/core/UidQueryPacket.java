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

import com.gigaspaces.internal.client.QueryResultTypeInternal;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.transport.AbstractProjectionTemplate;
import com.gigaspaces.internal.transport.AbstractQueryPacket;
import com.gigaspaces.internal.version.PlatformLogicalVersion;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Used for querying the space by a uid / uids.
 *
 * @author Niv Ingberg
 * @since 7.0
 */
@com.gigaspaces.api.InternalApi
public class UidQueryPacket extends AbstractQueryPacket {
    private static final long serialVersionUID = 1L;

    private String _uid;
    private String[] _uids;
    private boolean _isReturnOnlyUids;
    private int _version;
    private Object _routing;
    private String _typeName;
    private AbstractProjectionTemplate _projectionTemplate;

    /**
     * Empty constructor required by Externalizable.
     */
    public UidQueryPacket() {
    }

    public UidQueryPacket(ITypeDesc typeDesc, String uid, Object routing, int version, QueryResultTypeInternal resultType, boolean returnOnlyUids, AbstractProjectionTemplate projectionTemplate) {
        super(typeDesc, resultType);
        this._typeName = typeDesc != null ? typeDesc.getTypeName() : null;
        this._uid = uid;
        this._version = version;
        this._isReturnOnlyUids = returnOnlyUids;
        this._routing = routing;
        this._projectionTemplate = projectionTemplate;
    }

    public UidQueryPacket(String uid, Object routing, int version, QueryResultTypeInternal resultType) {
        this(null, uid, routing, version, resultType, false /*returnOnlyUids*/, null);
    }

    public UidQueryPacket(ITypeDesc typeDesc, String[] uids, int version, QueryResultTypeInternal resultType, boolean returnOnlyUids) {
        super(typeDesc, resultType);
        this._typeName = typeDesc != null ? typeDesc.getTypeName() : null;
        this._uids = uids;
        this._version = version;
        this._isReturnOnlyUids = returnOnlyUids;
        this._routing = null;
        this._projectionTemplate = null;
    }

    public UidQueryPacket(String[] uids, int version, QueryResultTypeInternal resultType, boolean returnOnlyUids) {
        this(null, uids, version, resultType, returnOnlyUids);
    }

    @Override
    public String getUID() {
        return _uid;
    }

    @Override
    public void setUID(String uid) {
        this._uid = uid;
    }

    @Override
    public String[] getMultipleUIDs() {
        return _uids;
    }

    @Override
    public void setMultipleUIDs(String[] uids) {
        this._uids = uids;
    }

    @Override
    public boolean isReturnOnlyUids() {
        return _isReturnOnlyUids;
    }

    @Override
    public String getTypeName() {
        return _typeName;
    }

    /**
     * UidQueryPacket can only be used to query the EDS if it is attached to specific type
     */
    @Override
    public boolean isTransient() {
        return _typeDesc == null || _typeDesc.getIdPropertyName() == null || super.isTransient();
    }

    @Override
    public int getVersion() {
        return _version;
    }

    @Override
    public void setVersion(int version) {
        this._version = version;
    }

    @Override
    public Object getRoutingFieldValue() {
        return _routing;
    }

    public void setRouting(Object routing) {
        _routing = routing;
    }

    @Override
    public boolean isIdQuery() {
        return _typeDesc != null;
    }

    private static final byte HAS_UID = 1 << 0;
    private static final byte HAS_UIDS = 1 << 1;
    private static final byte HAS_VERSION = 1 << 2;
    private static final byte RETURN_ONLY_UIDS = 1 << 3;
    private static final byte HAS_ROUTING = 1 << 4;
    private static final byte HAS_TYPE_NAME = 1 << 5;
    private static final byte HAS_PROJECTION = 1 << 6;

    @Override
    protected void readExternal(ObjectInput in, PlatformLogicalVersion version)
            throws IOException, ClassNotFoundException {
        super.readExternal(in, version);

        deserialize(in);
    }

    @Override
    protected void writeExternal(ObjectOutput out, PlatformLogicalVersion version) throws IOException {
        super.writeExternal(out, version);

        serialize(out, version);
    }

    @Override
    public void writeToSwap(ObjectOutput out) throws IOException {
        super.writeToSwap(out);
        serialize(out, PlatformLogicalVersion.getLogicalVersion());
    }

    private final void serialize(ObjectOutput out,
                                 PlatformLogicalVersion logicalVersion) throws IOException {
        byte flags = buildFlags(logicalVersion);
        out.writeByte(flags);
        if (_uid != null)
            IOUtils.writeString(out, _uid);
        if (_uids != null)
            IOUtils.writeStringArray(out, _uids);
        if (_version != 0)
            out.writeInt(_version);
        if (_routing != null)
            IOUtils.writeObject(out, _routing);
        if ((flags & HAS_TYPE_NAME) != 0)
            IOUtils.writeRepetitiveString(out, _typeName);
        if ((flags & HAS_PROJECTION) != 0)
            IOUtils.writeObject(out, _projectionTemplate);
    }

    @Override
    public void readFromSwap(ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readFromSwap(in);

        deserialize(in);
    }

    private final void deserialize(ObjectInput in) throws IOException, ClassNotFoundException {
        byte flags = in.readByte();

        _isReturnOnlyUids = ((flags & RETURN_ONLY_UIDS) != 0);

        if ((flags & HAS_UID) != 0)
            this._uid = IOUtils.readString(in);
        if ((flags & HAS_UIDS) != 0)
            this._uids = IOUtils.readStringArray(in);
        if ((flags & HAS_VERSION) != 0)
            this._version = in.readInt();
        if ((flags & HAS_ROUTING) != 0)
            this._routing = IOUtils.readObject(in);
        if ((flags & HAS_TYPE_NAME) != 0)
            this._typeName = IOUtils.readRepetitiveString(in);
        if ((flags & HAS_PROJECTION) != 0)
            this._projectionTemplate = IOUtils.readObject(in);
    }

    private byte buildFlags(PlatformLogicalVersion version) {
        byte flags = 0;
        if (_uid != null)
            flags |= HAS_UID;
        if (_uids != null)
            flags |= HAS_UIDS;
        if (_version != 0)
            flags |= HAS_VERSION;
        if (_isReturnOnlyUids)
            flags |= RETURN_ONLY_UIDS;
        if (_routing != null)
            flags |= HAS_ROUTING;
        if (_typeName != null && version.greaterOrEquals(PlatformLogicalVersion.v9_1_0))
            flags |= HAS_TYPE_NAME;
        if (_projectionTemplate != null && version.greaterOrEquals(PlatformLogicalVersion.v9_5_0))
            flags |= HAS_PROJECTION;
        return flags;
    }

    @Override
    public void setProjectionTemplate(AbstractProjectionTemplate projectionTemplate) {
        this._projectionTemplate = projectionTemplate;
    }

    @Override
    public AbstractProjectionTemplate getProjectionTemplate() {
        return _projectionTemplate;
    }

}

