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

import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Global unique identifier of a client operation. based on the LRMI id as the client ID and a
 * sequence number per client as the operation ID;
 *
 * @author asy ronen
 * @since 7.0
 */
public final class OperationID implements SmartExternalizable {

    private static final long serialVersionUID = 634112122503389345L;
    private long clientID;
    private long operationID;

    public OperationID(long clientID, long operationID) {
        this.clientID = clientID;
        this.operationID = operationID;
    }

    public OperationID() {
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;

        if (!(obj instanceof OperationID))
            return false;

        OperationID other = (OperationID) obj;
        return this.clientID == other.clientID && this.operationID == other.operationID;
    }

    @Override
    public int hashCode() {
        return (int) (clientID * 17 + operationID);
    }

    @Override
    public String toString() {
        return "ID:" + clientID + "." + operationID;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(clientID);
        out.writeLong(operationID);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        clientID = in.readLong();
        operationID = in.readLong();
    }

    public long getClientID() {
        return clientID;
    }

    public long getOperationID() {
        return operationID;
    }
}
