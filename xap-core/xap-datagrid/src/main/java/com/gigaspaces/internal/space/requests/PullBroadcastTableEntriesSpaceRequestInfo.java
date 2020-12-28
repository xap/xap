package com.gigaspaces.internal.space.requests;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.transport.IEntryPacket;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import static com.gigaspaces.internal.space.requests.BroadcastTableSpaceRequestInfo.Action.PULL_ENTRIES;
import static com.gigaspaces.internal.space.requests.BroadcastTableSpaceRequestInfo.Action.PUSH_ENTRIES;

/**
 * @author alon shoham
 * @since 15.8.0
 */
@com.gigaspaces.api.InternalApi
public class PullBroadcastTableEntriesSpaceRequestInfo extends BroadcastTableSpaceRequestInfo {
    private static final long serialVersionUID = 1L;
    private String typeName;

    public PullBroadcastTableEntriesSpaceRequestInfo() {
    }

    public PullBroadcastTableEntriesSpaceRequestInfo(String typeName) {
        this.typeName = typeName;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        IOUtils.writeString(out, typeName);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        this.typeName = IOUtils.readString(in);
    }

    @Override
    public Action getAction() {
        return PULL_ENTRIES;
    }

    public String getTypeName() {
        return typeName;
    }
}
