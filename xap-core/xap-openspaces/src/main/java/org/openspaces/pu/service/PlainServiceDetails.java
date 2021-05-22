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
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A simple implementation of {@link ServiceDetails}.
 *
 * @author kimchy
 */
public class PlainServiceDetails implements ServiceDetails, SmartExternalizable {

    private static final long serialVersionUID = -6051967441062799406L;

    protected String id;

    protected String serviceType;

    protected String serviceSubType;

    protected String description;

    protected String longDescription;

    protected Map<String, Object> attributes = new LinkedHashMap<String, Object>();

    // Just for externalizable
    public PlainServiceDetails() {
    }

    public PlainServiceDetails(String id, String serviceType, String serviceSubType,
                               String description, String longDescription) {
        this.id = id;
        this.serviceType = serviceType;
        this.serviceSubType = serviceSubType;
        this.description = description;
        if (this.description == null) {
            this.description = "";
        }
        this.longDescription = longDescription;
        if (this.longDescription == null) {
            this.longDescription = "";
        }
    }

    public String getId() {
        return this.id;
    }

    public String getServiceType() {
        return this.serviceType;
    }

    public String getServiceSubType() {
        return this.serviceSubType;
    }

    public String getDescription() {
        return this.description;
    }

    public String getLongDescription() {
        return this.longDescription;
    }

    public Map<String, Object> getAttributes() {
        return this.attributes;
    }

    public AggregatedServiceDetails aggregateByServiceType(ServiceDetails[] servicesDetails) {
        return null;
    }

    public AggregatedServiceDetails aggregateByServiceSubType(ServiceDetails[] servicesDetails) {
        return null;
    }

    public AggregatedServiceDetails aggregateById(ServiceDetails[] servicesDetails) {
        return null;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(id);
        out.writeUTF(serviceType);
        out.writeUTF(serviceSubType);
        out.writeUTF(description);
        out.writeUTF(longDescription);
        if (attributes == null) {
            out.writeInt(0);
        } else {
            out.writeInt(attributes.size());
            for (Map.Entry<String, Object> entry : attributes.entrySet()) {
                out.writeObject(entry.getKey());
                out.writeObject(entry.getValue());
            }
        }
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        id = in.readUTF();
        serviceType = in.readUTF();
        serviceSubType = in.readUTF();
        description = in.readUTF();
        longDescription = in.readUTF();
        attributes = new LinkedHashMap<String, Object>();
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
        sb.append("id[").append(id).append("] ");
        sb.append("type[").append(serviceType).append("/").append(serviceSubType).append("] ");
        for (Map.Entry<String, Object> entry : attributes.entrySet()) {
            sb.append(entry.getKey()).append("[").append(entry.getValue()).append("] ");
        }
        return sb.toString();
    }
}
