package com.gigaspaces.internal.server.space.suspend;

import java.io.*;

/**
 * Represents current suspend state of the space.
 * @author yohanakh
 * @since 14.0.0
 **/
public class SuspendInfo implements Externalizable {
    // serialVersionUID should never be changed.
    private static final long serialVersionUID = 1L;

    private SuspendType suspendType;

    public SuspendInfo() {
    }

    public SuspendInfo(SuspendType suspendType) {
        this.suspendType = suspendType;
    }


    public SuspendType getSuspendType() {
        return suspendType;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(suspendType);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        suspendType = (SuspendType) in.readObject();
    }
}
