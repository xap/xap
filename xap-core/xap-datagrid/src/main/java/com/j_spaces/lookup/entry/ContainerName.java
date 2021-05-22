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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Container name attributes for registering to LookupService. Every space registering with
 * <code>ContainerName</code> attribute.
 */
@com.gigaspaces.api.InternalApi
public class ContainerName extends com.j_spaces.lookup.entry.GenericEntry implements SmartExternalizable {
    private static final long serialVersionUID = -2623812164597953334L;

    public String name;

    public ContainerName() {
    }

    public ContainerName(String name) {
        if (name != null)
            this.name = name.intern();
    }

    @Override
    public net.jini.core.entry.Entry fromString(String attributeName) {
        return new ContainerName(attributeName);
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