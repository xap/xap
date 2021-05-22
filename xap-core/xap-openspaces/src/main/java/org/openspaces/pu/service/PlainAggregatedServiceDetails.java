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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A simple and generic implementation of the {@link org.openspaces.pu.service.AggregatedServiceDetails}
 * interface.
 *
 * @author kimchy
 */
public class PlainAggregatedServiceDetails implements AggregatedServiceDetails, SmartExternalizable {

    private static final long serialVersionUID = 706793192579879135L;

    private String serviceType;

    private Map<String, Object> attributes = new LinkedHashMap<String, Object>();

    /**
     * Just for externalizable.
     */
    public PlainAggregatedServiceDetails() {
    }

    public PlainAggregatedServiceDetails(String serviceType, ServiceDetails[] details) {
        this.serviceType = serviceType;
        if (details.length > 0) {
            attributes.putAll(details[0].getAttributes());
        }
    }

    public String getServiceType() {
        return this.serviceType;
    }

    public Map<String, Object> getAttributes() {
        return this.attributes;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(serviceType);
        out.writeInt(attributes.size());
        for (Map.Entry<String, Object> entry : attributes.entrySet()) {
            out.writeObject(entry.getKey());
            out.writeObject(entry.getValue());
        }
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        serviceType = in.readUTF();
        attributes = new HashMap<String, Object>();
        int attributesSize = in.readInt();
        for (int i = 0; i < attributesSize; i++) {
            String key = (String) in.readObject();
            Object value = in.readObject();
            attributes.put(key, value);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("type[").append(serviceType).append("] ");
        for (Map.Entry<String, Object> entry : attributes.entrySet()) {
            sb.append(entry.getKey()).append("[").append(entry.getValue()).append("] ");
        }
        return sb.toString();
    }
}
