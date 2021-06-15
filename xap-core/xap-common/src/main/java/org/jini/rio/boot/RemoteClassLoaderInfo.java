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
package org.jini.rio.boot;

import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Created by tamirs
 * on 11/14/16.
 */
public class RemoteClassLoaderInfo implements SmartExternalizable {
    private static final long serialVersionUID = 1L;

    private String name;
    private long loadTime;

    // Externalizable
    public RemoteClassLoaderInfo() {
    }

    public RemoteClassLoaderInfo(String name) {
        this.name = name;
        loadTime = -1;
    }

    public RemoteClassLoaderInfo(CodeChangeClassLoader codeChangeClassLoader) {
        name = codeChangeClassLoader.toString();
        loadTime = codeChangeClassLoader.getLoadTime();
    }

    public String getName() {
        return name;
    }

    public long getLoadTime() {
        return loadTime;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(name);
        out.writeLong(loadTime);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        name = (String) in.readObject();
        loadTime = in.readLong();
    }

    @Override
    public String toString() {
        return "RemoteClassLoaderInfo{" +
                "name='" + name + '\'' +
                (loadTime != -1 ? ", loadTime=" + loadTime : "") +
                '}';
    }
}
