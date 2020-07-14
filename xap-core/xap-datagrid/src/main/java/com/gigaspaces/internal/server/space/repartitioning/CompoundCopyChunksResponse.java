package com.gigaspaces.internal.server.space.repartitioning;

import com.gigaspaces.internal.io.IOUtils;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;

public class CompoundCopyChunksResponse extends CopyChunksResponseInfo {

    private List<CopyChunksResponseInfo> responses;

    public CompoundCopyChunksResponse() {
    }

    void addResponse(CopyChunksResponseInfo responseInfo) {
        if (this.responses == null) {
            this.responses = new ArrayList<>();
        }
        this.responses.add(responseInfo);
    }

    public List<CopyChunksResponseInfo> getResponses() {
        return responses;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeInt(out, responses.size());
        for (CopyChunksResponseInfo response : responses) {
            IOUtils.writeObject(out, response);
        }

    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        int size = IOUtils.readInt(in);
        this.responses = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            responses.add(IOUtils.readObject(in));
        }

    }
}
