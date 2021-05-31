package com.gigaspaces.internal.extension;

import com.gigaspaces.internal.space.requests.SpaceRequestInfo;
import com.j_spaces.core.SpaceContext;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class WaitForDataDrainRequest implements SpaceRequestInfo {

    private long timeout;
    private long minTimeToWait;
    private boolean backupOnly;
    private SpaceContext context;

    public WaitForDataDrainRequest() {
    }

    public WaitForDataDrainRequest(long timeout, long minTimeToWait, boolean backupOnly) {
        this.timeout = timeout;
        this.minTimeToWait = minTimeToWait;
        this.backupOnly = backupOnly;
    }

    public long getTimeout() {
        return timeout;
    }

    public boolean isBackupOnly() {
        return backupOnly;
    }

    @Override
    public SpaceContext getSpaceContext() {
        return context;
    }

    @Override
    public void setSpaceContext(SpaceContext spaceContext) {
        this.context = spaceContext;
    }

    public long getMinTimeToWait() {
        return minTimeToWait;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(timeout);
        out.writeLong(minTimeToWait);
        out.writeBoolean(backupOnly);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.timeout = in.readLong();
        this.minTimeToWait = in.readLong();
        this.backupOnly = in.readBoolean();

    }
}
