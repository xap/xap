package org.jini.rio.boot;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Created by tamirs
 * on 11/14/16.
 */
public class RemoteClassLoaderInfo implements Externalizable {
    private static final long serialVersionUID = 1L;

    private String name;

    // Externalizable
    public RemoteClassLoaderInfo() {
    }

    public RemoteClassLoaderInfo(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(name);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        name = (String) in.readObject();
    }

    @Override
    public String toString() {
        return "RemoteClassLoaderInfo{" +
                "name='" + name + '\'' +
                '}';
    }
}
