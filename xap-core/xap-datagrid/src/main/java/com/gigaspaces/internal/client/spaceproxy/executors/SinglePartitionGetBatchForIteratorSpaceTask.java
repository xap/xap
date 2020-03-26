package com.gigaspaces.internal.client.spaceproxy.executors;

import com.gigaspaces.client.iterator.cursor.SpaceIteratorBatchResultProvider;
import com.gigaspaces.internal.client.SpaceIteratorBatchResult;
import com.gigaspaces.internal.space.requests.GetBatchForIteratorSpaceRequestInfo;
import com.gigaspaces.internal.space.requests.SpaceRequestInfo;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author Alon Shoham
 * @since 15.2.0
 */
@com.gigaspaces.api.InternalApi
public class SinglePartitionGetBatchForIteratorSpaceTask extends SystemTask<SpaceIteratorBatchResult> {
    private static final long serialVersionUID = 1L;

    private GetBatchForIteratorSpaceRequestInfo _getBatchForIteratorSpaceRequestInfo;

    public SinglePartitionGetBatchForIteratorSpaceTask() {
    }

    public SinglePartitionGetBatchForIteratorSpaceTask(SpaceIteratorBatchResultProvider spaceIteratorBatchResultProvider, int batchNumber) {
        _getBatchForIteratorSpaceRequestInfo = new GetBatchForIteratorSpaceRequestInfo(spaceIteratorBatchResultProvider.getQueryPacket(), spaceIteratorBatchResultProvider.getReadModifiers(), spaceIteratorBatchResultProvider.getBatchSize(), batchNumber, spaceIteratorBatchResultProvider.getUuid(), spaceIteratorBatchResultProvider.getMaxInactiveDuration());
    }

    @Override
    public SpaceRequestInfo getSpaceRequestInfo() {
        return _getBatchForIteratorSpaceRequestInfo;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(_getBatchForIteratorSpaceRequestInfo);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        this._getBatchForIteratorSpaceRequestInfo = (GetBatchForIteratorSpaceRequestInfo) in.readObject();
    }
}
