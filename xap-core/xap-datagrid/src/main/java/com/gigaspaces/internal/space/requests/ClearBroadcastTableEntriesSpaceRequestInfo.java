package com.gigaspaces.internal.space.requests;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.transport.ITemplatePacket;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import static com.gigaspaces.internal.space.requests.BroadcastTableSpaceRequestInfo.Action.CLEAR_ENTRIES;
import static com.gigaspaces.internal.space.requests.BroadcastTableSpaceRequestInfo.Action.PUSH_ENTRIES;

/**
 * @author alon shoham
 * @since 15.8.0
 */
@com.gigaspaces.api.InternalApi
public class ClearBroadcastTableEntriesSpaceRequestInfo extends BroadcastTableSpaceRequestInfo {
    private static final long serialVersionUID = 1L;
    private ITemplatePacket templatePacket;
    private int modifiers;

    public ClearBroadcastTableEntriesSpaceRequestInfo() {
    }

    public ClearBroadcastTableEntriesSpaceRequestInfo(ITemplatePacket templatePacket, int modifiers) {
        this.templatePacket = templatePacket;
        this.modifiers = modifiers;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        IOUtils.writeObject(out, templatePacket);
        IOUtils.writeInt(out, modifiers);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        templatePacket = IOUtils.readObject(in);
        modifiers = IOUtils.readInt(in);
    }

    public ITemplatePacket getTemplatePacket() {
        return templatePacket;
    }

    public int getModifiers() {
        return modifiers;
    }

    @Override
    public Action getAction() {
        return CLEAR_ENTRIES;
    }
}
