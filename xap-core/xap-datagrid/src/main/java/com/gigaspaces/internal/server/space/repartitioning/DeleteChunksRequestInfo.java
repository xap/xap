package com.gigaspaces.internal.server.space.repartitioning;

import com.gigaspaces.admin.quiesce.QuiesceToken;
import com.gigaspaces.internal.cluster.ClusterTopology;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.space.requests.SpaceRequestInfo;
import com.j_spaces.core.SpaceContext;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class DeleteChunksRequestInfo implements SpaceRequestInfo {
    private static final long serialVersionUID = 4826314985077083352L;
    private ClusterTopology newMap;
    private QuiesceToken token;

    DeleteChunksRequestInfo(ClusterTopology newMap, QuiesceToken token) {
        this.newMap = newMap;
        this.token = token;
    }

    public DeleteChunksRequestInfo() {
    }

    public ClusterTopology getNewMap() {
        return newMap;
    }

    public QuiesceToken getToken() {
        return token;
    }

    @Override
    public SpaceContext getSpaceContext() {
        return null;
    }

    @Override
    public void setSpaceContext(SpaceContext spaceContext) {

    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeObject(out, newMap);
        IOUtils.writeObject(out, token);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.newMap = IOUtils.readObject(in);
        this.token = IOUtils.readObject(in);
    }
}
