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

package com.gigaspaces.cluster.activeelection.core;

import com.gigaspaces.serialization.SmartExternalizable;
import com.j_spaces.lookup.entry.GenericEntry;

import net.jini.core.entry.Entry;
import net.jini.lookup.entry.ServiceControlled;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


/**
 * The election state <i>attribute</i> registered in conjunction with service candidate. This state
 * managed by {@link ActiveElectionManager} and modified on Naming service if the candidate service
 * aquire a new state. <p> The following states are available: <li>NONE 	- The service is not
 * initialized yet.</li> <li>PENDING - The service is a candidate to acquire PREPARE state.</li>
 * <li>PREPARE - The service is a candidate to acquire an ACTIVE state.</li> <li>ACTIVE  - The
 * service become to be an ACTIVE.</li> <p>
 *
 * <b>NOTE:</b> This state is {@link ServiceControlled} and can be modified only by service itself.
 *
 * @author Igor Goldenberg
 * @version 1.0
 * @see ActiveElectionManager
 * @see State
 * @since 5.2
 */
@com.gigaspaces.api.InternalApi
public class ActiveElectionState
        extends GenericEntry
        implements ServiceControlled, SmartExternalizable {
    private static final long serialVersionUID = 1L;

    /**
     * Service states
     */
    public static enum State {
        NONE, PENDING, PREPARE, ACTIVE;
    }

    public String state;

    /**
     * public no-args constructor
     */
    public ActiveElectionState() {
    }

    /**
     * @return the current state
     */
    public State getState() {
        return State.valueOf(state);
    }

    /**
     * construct this ActiveElectionState with the given State
     *
     * @param state State of this active election
     */
    public ActiveElectionState(State state) {
        setState(state);
    }

    public void setState(State state) {
        if (state != null)
            this.state = state.name().intern(); // keep as String to be compatible with 1.4
    }

    /*
     * @see com.j_spaces.lookup.entry.GenericEntry#fromString(java.lang.String)
     */
    @Override
    public Entry fromString(String state) {
        return new ActiveElectionState(ActiveElectionState.State.valueOf(state));
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(state);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        state = (String) in.readObject();
    }
}