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
