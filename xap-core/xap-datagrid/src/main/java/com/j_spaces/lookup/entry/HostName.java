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
import net.jini.core.entry.Entry;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * The hostName attributes for registering to LookupService. Every container or/and space may be
 * registered with <code>HostName</code> attribute.
 */
@com.gigaspaces.api.InternalApi
public class HostName extends GenericEntry implements SmartExternalizable {
    private static final long serialVersionUID = 3654894087785340873L;

    public String name;

    public HostName() {
    }

    public HostName(String name) {
        if (name != null)
            this.name = name.intern();
    }

    /**
     * @return the hostname value from arrays of <code>attrs</code>, <code>null</code> if no
     * HostName entry as part of <code>attrs</code>.
     **/
    public static String getHostNameFrom(Entry[] attrs) {
        if (attrs == null)
            return null;

        for (Entry e : attrs) {
            if (e instanceof HostName)
                return ((HostName) e).name;
        }

        return null; // couldn't find
    }

    @Override
    public Entry fromString(String attributeName) {
        return new HostName(attributeName);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(name);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        name = (String) in.readObject();
    }
}
