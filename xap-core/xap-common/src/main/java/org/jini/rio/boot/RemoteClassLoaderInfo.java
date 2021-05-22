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
