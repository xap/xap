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
import com.j_spaces.core.JSpaceState;

import net.jini.core.entry.Entry;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * <code> The State entry represents the current service state. The State has primary state - @see
 * com.j_spaces.core.ISpaceState and secondary states. Secondary states are internal states and
 * shouldn't be used in space lookup.
 *
 * electable - state that indicates whether the space can participate in election . replicable -
 * state that indicates whether the space is ready for replication. </code>
 *
 * @see com.j_spaces.core.ISpaceState
 * @since 5.0
 */
@com.gigaspaces.api.InternalApi
public class State extends GenericEntry implements SmartExternalizable {
    private static final long serialVersionUID = 1L;

    public String state;
    public Boolean electable;
    public Boolean replicable;


    public State(int state, Boolean electable,
                 Boolean replicable) {
        this(state);

        this.electable = electable;
        this.replicable = replicable;
    }


    /**
     * Construct a State
     *
     * @param state A state value
     */
    public State(String state) {
        if (state == null)
            throw new NullPointerException("state is null");

        this.state = state.intern();
    }

    /**
     * Construct a State
     */
    public State(int state) {
        this(JSpaceState.convertToString(Integer.valueOf(state)));
    }

    /**
     * Construct a State with null attributes
     */
    public State() {
    }

    /**
     * @return true if the state is electable
     */
    public Boolean isElectable() {
        return electable;
    }

    /**
     * This state is changed to true on space startup and set to false only at space shutdown or
     * stop
     */
    public void setElectable(Boolean electable) {
        this.electable = electable;
    }


    public Boolean getReplicable() {
        return replicable;
    }


    /**
     * This state is set to true after space recovery and set to false when space is stopped or
     * shutdown
     */
    public State setReplicable(Boolean replicable) {
        this.replicable = replicable;
        return this;
    }


    @Override
    public Entry fromString(String state) {
        return new State(state);
    }

    private interface BitMap {
        byte STATE = 1 << 0;
        byte ELECTABLE = 1 << 1;
        byte REPLICATABLE = 1 << 2;
    }


    public void writeExternal(ObjectOutput out) throws IOException {
        final byte flags = buildFlags();
        out.writeByte(flags);

        if (flags == 0)
            return;

        if (state != null) {
            out.writeObject(state);
        }
        if (electable != null) {
            out.writeBoolean(electable);
        }
        if (replicable != null) {
            out.writeBoolean(replicable);
        }
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        final byte flags = in.readByte();

        if (flags == 0)
            return;

        if ((flags & BitMap.STATE) != 0) {
            state = (String) in.readObject();
        }

        if ((flags & BitMap.ELECTABLE) != 0) {
            electable = in.readBoolean();
        }

        if ((flags & BitMap.REPLICATABLE) != 0) {
            replicable = in.readBoolean();
        }
    }

    private byte buildFlags() {
        byte flags = 0;
        if (state != null)
            flags |= BitMap.STATE;
        if (electable != null)
            flags |= BitMap.ELECTABLE;
        if (replicable != null)
            flags |= BitMap.REPLICATABLE;
        return flags;
    }
}
