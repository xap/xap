package com.gigaspaces.internal.server.space.repartitioning;

import com.gigaspaces.admin.quiesce.QuiesceToken;
import com.gigaspaces.internal.cluster.PartitionToChunksMap;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.space.requests.SpaceRequestInfo;
import com.j_spaces.core.SpaceContext;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Map;

public class CopyChunksRequestInfo implements SpaceRequestInfo {

    private SpaceContext context;
    private PartitionToChunksMap newMap;
    private String spaceName;
    private Map<Integer, String> instanceIds;
    private QuiesceToken token;

    public CopyChunksRequestInfo() {
    }

    CopyChunksRequestInfo(PartitionToChunksMap newMap, String spaceName, Map<Integer, String> instanceIds, QuiesceToken token) {
        this.newMap = newMap;
        this.spaceName = spaceName;
        this.instanceIds = instanceIds;
        this.token = token;
    }

    public PartitionToChunksMap getNewMap() {
        return newMap;
    }

    public String getSpaceName() {
        return spaceName;
    }

    Map<Integer, String> getInstanceIds() {
        return instanceIds;
    }

    public QuiesceToken getToken() {
        return token;
    }

    @Override
    public SpaceContext getSpaceContext() {
        return this.context;
    }

    @Override
    public void setSpaceContext(SpaceContext spaceContext) {
        this.context = spaceContext;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeObject(out, newMap);
        IOUtils.writeString(out, spaceName);
        IOUtils.writeObject(out, token);
        IOUtils.writeShort(out, (short) instanceIds.size());
        for (Map.Entry<Integer, String> entry : instanceIds.entrySet()) {
            IOUtils.writeShort(out, entry.getKey().shortValue());
            IOUtils.writeString(out, entry.getValue());
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.newMap = IOUtils.readObject(in);
        this.spaceName = IOUtils.readString(in);
        this.token = IOUtils.readObject(in);
        short size = IOUtils.readShort(in);
        this.instanceIds = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            instanceIds.put((int) IOUtils.readShort(in), IOUtils.readString(in));
        }
    }
}
