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

package com.gigaspaces.management.entry;

import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Used as a means for a service to publish information about a JMX Connector.
 **/
@com.gigaspaces.api.InternalApi
public class JMXConnection extends net.jini.entry.AbstractEntry implements SmartExternalizable {
    private static final long serialVersionUID = 1L;
    /**
     * The name of the property.
     */
    public String jmxServiceURL;
    /**
     * Human readable name for the connector
     */
    public String name;

    /**
     * Construct an empty instance of this class.
     **/
    public JMXConnection() {
    }

    /**
     * Construct an instance of the JMXConnection, with all fields initialized
     *
     * @param jmxServiceURL String value of the {@link javax.management.remote.JMXServiceURL}
     * @param name          A Human readable name for the connector
     */
    public JMXConnection(String jmxServiceURL, String name) {
        this.jmxServiceURL = jmxServiceURL;
        this.name = name;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(jmxServiceURL);
        out.writeObject(name);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        jmxServiceURL = (String) in.readObject();
        name = (String) in.readObject();
    }

}