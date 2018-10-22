package com.gigaspaces.internal.server.space.quiesce;

import java.io.Serializable;

public class SuspendInfo implements Serializable {
    private SuspendType suspendType;

    public SuspendInfo() {
    }

    public SuspendInfo(SuspendType suspendType) {
        this.suspendType = suspendType;
    }


    public SuspendType getSuspendType() {
        return suspendType;
    }

    public void setSuspendType(SuspendType suspendType) {
        this.suspendType = suspendType;
    }
}
