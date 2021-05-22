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


package com.gigaspaces.admin.quiesce;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Represents an event which is fired to inform about quiesce state changed
 *
 * @author Boris
 * @since 10.1.0
 */
@com.gigaspaces.api.InternalApi
public class QuiesceStateChangedEvent implements SmartExternalizable {

    private static final long serialVersionUID = 1L;
    private QuiesceState quiesceState;
    private QuiesceToken token;
    private String description;

    public QuiesceStateChangedEvent() {
    }

    public QuiesceStateChangedEvent(QuiesceState quiesceState, QuiesceToken token, String description) {
        this.quiesceState = quiesceState;
        this.token = token;
        this.description = description;
    }

    public QuiesceState getQuiesceState() {
        return quiesceState;
    }

    public QuiesceToken getToken() {
        return token;
    }

    public String getDescription() {
        return description;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(quiesceState.getCode());
        IOUtils.writeObject(out, token);
        IOUtils.writeString(out, description);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        int quiesceStateCode = in.readInt();
        quiesceState = QuiesceState.traslateCodeToState(quiesceStateCode);
        token = IOUtils.readObject(in);
        description = IOUtils.readString(in);
    }
}
