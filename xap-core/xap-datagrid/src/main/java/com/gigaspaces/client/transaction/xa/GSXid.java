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
 * Created on 28/03/2005
 */
package com.gigaspaces.client.transaction.xa;

import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.UUID;

import javax.transaction.xa.Xid;

/**
 * Local implementation of Xid to be store in Space
 */
@com.gigaspaces.api.InternalApi
public class GSXid implements Xid, SmartExternalizable {
    static final long serialVersionUID = 836721541656259722L;

    private byte[] _branchQualifier;
    private int _formatId;
    private byte[] _globalTransactionId;
    private transient volatile int _hashCode = 0;
    private UUID _rmid;

    /**
     * Empty constructor for <code>Externalizable</code>
     */
    public GSXid() {
    }

    /**
     * Ctor
     */
    public GSXid(byte[] branchQualifier,
                 int formatId, byte[] globalTransactionId) {
        _branchQualifier = branchQualifier;
        _formatId = formatId;
        _globalTransactionId = globalTransactionId;
    }

    /**
     * Ctor - copy constructor
     */
    public GSXid(Xid xid) {
        this(xid.getBranchQualifier(), xid.getFormatId(), xid.getGlobalTransactionId());
    }

    public GSXid(Xid xid, UUID rmid) {
        this(xid);
        _rmid = rmid;
        hashCode();
    }

    /* (non-Javadoc)
     * @see javax.transaction.xa.Xid#getGlobalTransactionId()
     */
    @Override
    public byte[] getBranchQualifier() {
        return _branchQualifier;
    }

    /* (non-Javadoc)
     * @see javax.transaction.xa.Xid#getFormatId()
     */
    @Override
    public int getFormatId() {
        return _formatId;
    }

    /* (non-Javadoc)
     * @see javax.transaction.xa.Xid#getGlobalTransactionId()
     */
    @Override
    public byte[] getGlobalTransactionId() {
        return _globalTransactionId;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#equals( Object)
     */
    @Override
    public boolean equals(Object obj) {
        boolean res = false;
        if (obj instanceof Xid) {
            Xid xid = (Xid) obj;
            res = Arrays.equals(xid.getBranchQualifier(), _branchQualifier) &&
                    xid.getFormatId() == _formatId &&
                    Arrays.equals(xid.getGlobalTransactionId(), _globalTransactionId);
            if (res && obj instanceof GSXid) {
                GSXid other = (GSXid) obj;
                if (_rmid != null)
                    res = other._rmid != null && _rmid.equals(other._rmid);
                else
                    res = other._rmid == null;
            }
        }
        return res;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        if (_hashCode != 0)
            return _hashCode;
        for (int i = _globalTransactionId.length - 1; i >= 0; i--)
            _hashCode += _globalTransactionId[i] & 0xff;

        if (_hashCode == 0)
            _hashCode = 1;
        return _hashCode;
    }

    /* (non-Javadoc)
     * @see java.io.Externalizable#writeExternal(ObjectOutput) 
     */
    @Override
    public void writeExternal(ObjectOutput objectoutput) throws IOException {
        objectoutput.writeInt(_formatId);
        objectoutput.writeByte((byte) _globalTransactionId.length);
        objectoutput.write(_globalTransactionId);
        if (_branchQualifier == null) {
            objectoutput.writeByte(-1);
        } else {
            objectoutput.writeByte((byte) _branchQualifier.length);
            objectoutput.write(_branchQualifier);
        }
        objectoutput.writeBoolean(_rmid != null);
        if (_rmid != null)
            objectoutput.writeObject(_rmid);
    }

    /* (non-Javadoc)
     * @see java.io.Externalizable#readExternal(ObjectInput) 
     */
    @Override
    public void readExternal(ObjectInput objectinput) throws IOException, ClassNotFoundException {
        _formatId = objectinput.readInt();
        byte bytecCount = objectinput.readByte();
        if (bytecCount < 0)
            throw new IOException("Stream error.");
        _globalTransactionId = new byte[bytecCount];
        objectinput.readFully(_globalTransactionId);
        bytecCount = objectinput.readByte();
        if (bytecCount < -1)
            throw new IOException("Stream error.");
        if (bytecCount > -1) {
            _branchQualifier = new byte[bytecCount];
            objectinput.readFully(_branchQualifier);
        }

        if (objectinput.readBoolean())
            _rmid = (UUID) objectinput.readObject();
        hashCode();
    }
}
