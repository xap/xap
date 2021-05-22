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

package org.openspaces.pu.service;

import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A simple implementation of {@link ServiceMonitors}.
 *
 * @author kimchy
 */
public class PlainServiceMonitors implements ServiceMonitors, SmartExternalizable {

    private static final long serialVersionUID = -2530051320077906919L;

    protected String id;

    protected ServiceDetails details;

    protected Map<String, Object> monitors;

    // Just for externalizable

    public PlainServiceMonitors() {
    }

    public PlainServiceMonitors(String id) {
        this.id = id;
        this.monitors = new LinkedHashMap<String, Object>();
    }

    public PlainServiceMonitors(String id, Map<String, Object> monitors) {
        this.id = id;
        this.monitors = new LinkedHashMap<String, Object>(monitors);
    }

    public String getId() {
        return this.id;
    }

    public Map<String, Object> getMonitors() {
        return this.monitors;
    }

    public ServiceDetails getDetails() {
        return details;
    }

    public void setDetails(ServiceDetails details) {
        this.details = details;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(id);
        if (monitors == null) {
            out.writeInt(0);
        } else {
            out.writeInt(monitors.size());
            for (Map.Entry<String, Object> entry : monitors.entrySet()) {
                out.writeObject(entry.getKey());
                out.writeObject(entry.getValue());
            }
        }
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        id = in.readUTF();
        int attributesSize = in.readInt();
        if (attributesSize == 0) {
            monitors = Collections.EMPTY_MAP;
        } else {
            monitors = new LinkedHashMap<String, Object>();
            for (int i = 0; i < attributesSize; i++) {
                String key = (String) in.readObject();
                Object value = in.readObject();
                monitors.put(key, value);
            }
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("id[").append(id).append("] ");
        for (Map.Entry<String, Object> entry : monitors.entrySet()) {
            sb.append(entry.getKey()).append("[").append(entry.getValue()).append("] ");
        }
        return sb.toString();
    }
}
