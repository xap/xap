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

package com.j_spaces.lookup.entry;

import com.gigaspaces.serialization.SmartExternalizable;
import net.jini.lookup.entry.ServiceControlled;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author Guy Korland
 * @version 0.1
 * @since 5.1
 */
@com.gigaspaces.api.InternalApi
public class JoinState extends net.jini.entry.AbstractEntry implements ServiceControlled, SmartExternalizable {
    private static final long serialVersionUID = -6689429246684479833L;

    public Integer _state;

    public JoinState() {
    }

    public JoinState(State state) {
        _state = state.ordinal();
    }

    public static enum State {
        None, Pending, Prepare, Active;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        if (_state == null)
            out.writeInt(Integer.MIN_VALUE);
        else
            out.writeInt(_state);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        int state = in.readInt();
        if (state != Integer.MIN_VALUE)
            _state = state;
    }
}
