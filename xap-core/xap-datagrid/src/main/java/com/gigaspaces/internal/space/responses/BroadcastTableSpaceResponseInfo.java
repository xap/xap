package com.gigaspaces.internal.space.responses;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.transport.IEntryPacket;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Map;

/**
 * @author alon shoham
 * @since 15.8.0
 */
@com.gigaspaces.api.InternalApi
public class BroadcastTableSpaceResponseInfo extends AbstractSpaceResponseInfo {
    private static final long serialVersionUID = 1L;
    private Map<Integer, Exception> exceptionMap = new HashMap<>();
    private IEntryPacket[] entries;

    public BroadcastTableSpaceResponseInfo() {
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        IOUtils.writeObject(out, exceptionMap);
        IOUtils.writeObject(out, entries);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        exceptionMap = IOUtils.readObject(in);
        entries = IOUtils.readObject(in);
    }

    public void addException(Integer partitionId, Exception e){
        exceptionMap.put(partitionId, e);
    }

    public Map<Integer, Exception> getExceptionMap() {
        return exceptionMap;
    }

    public boolean finishedSuccessfully(){
        return exceptionMap.isEmpty();
    }

    public IEntryPacket[] getEntries() {
        return entries;
    }

    public void setEntries(IEntryPacket[] newEntries){
        this.entries = newEntries;
    }
}
