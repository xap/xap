package com.gigaspaces.internal.space.requests;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.transport.IEntryPacket;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import static com.gigaspaces.internal.space.requests.BroadcastTableSpaceRequestInfo.Action.PUSH_ENTRY;

/**
 * @author alon shoham
 * @since 15.8.0
 */
@com.gigaspaces.api.InternalApi
public class PushBroadcastTableEntrySpaceRequestInfo extends BroadcastTableSpaceRequestInfo {
    private static final long serialVersionUID = 1L;
    private IEntryPacket entryPacket;
    private long lease;
    private boolean isUpdate;
    private long timeout;
    private int modifiers;

    public PushBroadcastTableEntrySpaceRequestInfo() {
    }

    public PushBroadcastTableEntrySpaceRequestInfo(IEntryPacket entryPacket, long lease, boolean isUpdate, long timeout, int modifiers) {
        this.entryPacket = entryPacket;
        this.lease = lease;
        this.isUpdate = isUpdate;
        this.timeout = timeout;
        this.modifiers = modifiers;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        IOUtils.writeObject(out, entryPacket);
        IOUtils.writeLong(out, lease);
        out.writeBoolean(isUpdate);
        IOUtils.writeLong(out, timeout);
        IOUtils.writeInt(out, modifiers);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        entryPacket = IOUtils.readObject(in);
        lease = IOUtils.readLong(in);
        isUpdate = in.readBoolean();
        timeout = IOUtils.readLong(in);
        modifiers = IOUtils.readInt(in);
    }

    public IEntryPacket getEntry() {
        return entryPacket;
    }

    public long getLease() {
        return lease;
    }

    public boolean isUpdate() {
        return isUpdate;
    }

    public int getModifiers() {
        return modifiers;
    }

    public long getTimeout() {
        return timeout;
    }

    @Override
    public Action getAction() {
        return PUSH_ENTRY;
    }
}
