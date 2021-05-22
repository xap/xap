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

package com.gigaspaces.internal.transaction;

import com.gigaspaces.serialization.SmartExternalizable;
import com.gigaspaces.transaction.TransactionUniqueId;

import net.jini.id.Uuid;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author idan
 * @since 9.0.1
 */
public abstract class AbstractTransactionUniqueId implements TransactionUniqueId, SmartExternalizable {
    private static final long serialVersionUID = 1L;

    private Uuid _transactionManagerId;

    protected AbstractTransactionUniqueId() {
    }

    protected AbstractTransactionUniqueId(Uuid transactionManagerId) {
        this._transactionManagerId = transactionManagerId;
    }

    @Override
    public Uuid getTransactionManagerId() {
        return _transactionManagerId;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(_transactionManagerId);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        _transactionManagerId = (Uuid) in.readObject();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((_transactionManagerId == null) ? 0 : _transactionManagerId.hashCode());
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
        AbstractTransactionUniqueId other = (AbstractTransactionUniqueId) obj;
        if (_transactionManagerId == null) {
            if (other._transactionManagerId != null)
                return false;
        } else if (!_transactionManagerId.equals(other._transactionManagerId))
            return false;
        return true;
    }

}
