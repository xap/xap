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

package com.gigaspaces.client;

import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Holds a read/takeByIds operation's exception result.
 *
 * @author idan
 * @since 7.1.1
 */
@com.gigaspaces.api.InternalApi
public class ReadTakeByIdResult implements SmartExternalizable {

    private static final long serialVersionUID = 1L;
    protected Object _id;
    protected Object _readObject;
    protected Throwable _error;

    public ReadTakeByIdResult() {
    }

    public ReadTakeByIdResult(Object id, Object readObject, Throwable error) {
        _id = id;
        _readObject = readObject;
        _error = error;
    }

    public Throwable getError() {
        return _error;
    }

    public Object getId() {
        return _id;
    }

    public Object getObject() {
        return _readObject;
    }

    public boolean isError() {
        return _error != null;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(_id);
        out.writeObject(_readObject);
        out.writeObject(_error);
    }

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        _id = in.readObject();
        _readObject = in.readObject();
        _error = (Throwable) in.readObject();
    }

}
