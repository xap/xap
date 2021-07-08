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
import java.util.HashMap;

/**
 * Created by tamirs
 * on 11/14/16.
 */
public class SpaceInstanceRemoteClassLoaderInfo implements SmartExternalizable {

    private static final long serialVersionUID = 1L;

    private RemoteClassLoaderInfo defaultClassLoader;
    private HashMap<String, RemoteClassLoaderInfo> classLoadersInfo; // version --> classloader info
    int maxClassLoaders;

    // Externalizable
    public SpaceInstanceRemoteClassLoaderInfo() {
    }

    public SpaceInstanceRemoteClassLoaderInfo(RemoteClassLoaderInfo defaultClassLoader, HashMap<String, RemoteClassLoaderInfo> classLoadersInfo, int maxClassLoaders) {
        this.defaultClassLoader = defaultClassLoader;
        this.classLoadersInfo = classLoadersInfo;
        this.maxClassLoaders = maxClassLoaders;
    }

    public RemoteClassLoaderInfo getDefaultClassLoader() {
        return defaultClassLoader;
    }

    public HashMap<String, RemoteClassLoaderInfo> getClassLoadersInfo() {
        return classLoadersInfo;
    }

    public int getMaxClassLoaders() {
        return maxClassLoaders;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(defaultClassLoader);
        out.writeObject(classLoadersInfo);
        out.writeInt(maxClassLoaders);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        defaultClassLoader = (RemoteClassLoaderInfo) in.readObject();
        classLoadersInfo = (HashMap<String, RemoteClassLoaderInfo>) in.readObject();
        maxClassLoaders = in.readInt();
    }

    @Override
    public String toString() {
        return "SpaceInstanceRemoteClassLoaderInfo{" +
                "defaultClassLoader=" + defaultClassLoader +
                ", classLoadersInfo=" + classLoadersInfo +
                ", maxClassLoaders=" + maxClassLoaders +
                '}';
    }
}
