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


package com.j_spaces.core.admin;

import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Date;

/**
 * Used to hold a template info.
 *
 * @author Guyk
 * @see com.j_spaces.core.admin.IInternalRemoteJSpaceAdmin#getTemplatesInfo(String)
 * @since 6.0
 */
@com.gigaspaces.api.InternalApi
public class TemplateInfo implements SmartExternalizable {
    private static final long serialVersionUID = -4483974697687984245L;

    private boolean _isFIFO;
    private Date _expiresAt;
    private Object[] _values;
    private String _waitingUID;
    private int _notifyType;

    public TemplateInfo() {
    }

    public TemplateInfo(boolean isFIFO, Date expiresAt, Object[] values, String waitingUID, int notifyType) {
        _isFIFO = isFIFO;
        _expiresAt = expiresAt;
        _values = values;
        _waitingUID = waitingUID;
        _notifyType = notifyType;
    }

    /*
     * @see java.io.Externalizable#readExternal(java.io.ObjectInput)
     */
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        _isFIFO = in.readBoolean();
        _expiresAt = (Date) in.readObject();
        _values = (Object[]) in.readObject();
        _waitingUID = (String) in.readObject();
        _notifyType = in.readInt();
    }

    /*
     * @see java.io.Externalizable#writeExternal(java.io.ObjectOutput)
     */
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeBoolean(_isFIFO);
        out.writeObject(_expiresAt);
        out.writeObject(_values);
        out.writeObject(_waitingUID);
        out.writeInt(_notifyType);
    }

    public boolean isFIFO() {
        return _isFIFO;
    }

    public Date getExpiresAt() {
        return _expiresAt;
    }

    public Object[] getValues() {
        return _values;
    }

    public String getWaitingUID() {
        return _waitingUID;
    }

}

