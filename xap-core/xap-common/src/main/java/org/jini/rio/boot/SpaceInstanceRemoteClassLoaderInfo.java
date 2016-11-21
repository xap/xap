package org.jini.rio.boot;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;

/**
 * Created by tamirs
 * on 11/14/16.
 */
public class SpaceInstanceRemoteClassLoaderInfo implements  Externalizable {

    private static final long serialVersionUID = 1L;

    private RemoteClassLoaderInfo defaultClassLoader;
    private HashMap<String, RemoteClassLoaderInfo> classLoadersInfo; // version --> classloader info

    // Externalizable
    public SpaceInstanceRemoteClassLoaderInfo() {
    }

    public SpaceInstanceRemoteClassLoaderInfo(RemoteClassLoaderInfo defaultClassLoader, HashMap<String, RemoteClassLoaderInfo> classLoadersInfo) {
        this.defaultClassLoader = defaultClassLoader;
        this.classLoadersInfo = classLoadersInfo;
    }

    public RemoteClassLoaderInfo getDefaultClassLoader() {
        return defaultClassLoader;
    }

    public HashMap<String, RemoteClassLoaderInfo> getClassLoadersInfo() {
        return classLoadersInfo;
    }


    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(defaultClassLoader);
        out.writeObject(classLoadersInfo);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        defaultClassLoader = (RemoteClassLoaderInfo) in.readObject();
        classLoadersInfo = (HashMap<String, RemoteClassLoaderInfo>) in.readObject();
    }

    @Override
    public String toString() {
        return "SpaceInstanceRemoteClassLoaderInfo{" +
                "defaultClassLoader=" + defaultClassLoader +
                ", classLoadersInfo=" + classLoadersInfo +
                '}';
    }
}
