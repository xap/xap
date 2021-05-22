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

package org.openspaces.pu.container.servicegrid;

import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author kimchy
 */
public class PUMonitors implements SmartExternalizable {

    private static final long serialVersionUID = -2151337794076639780L;

    private long timestamp;

    private Object[] monitors;

    public PUMonitors() {
    }

    public PUMonitors(Object[] monitors) {
        this.timestamp = System.currentTimeMillis();
        this.monitors = monitors;
    }

    public Object[] getMonitors() {
        return monitors;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(timestamp);
        out.writeInt(monitors.length);
        for (Object monitor : this.monitors) {
            out.writeObject(monitor);
        }
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        timestamp = in.readLong();
        monitors = new Object[in.readInt()];
        for (int i = 0; i < monitors.length; i++) {
            monitors[i] = in.readObject();
        }
    }
}
