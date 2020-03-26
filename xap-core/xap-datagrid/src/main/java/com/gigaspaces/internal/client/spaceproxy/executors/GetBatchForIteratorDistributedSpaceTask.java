package com.gigaspaces.internal.client.spaceproxy.executors;

import com.gigaspaces.async.AsyncResult;
import com.gigaspaces.async.AsyncResultFilter;
import com.gigaspaces.async.AsyncResultFilterEvent;
import com.gigaspaces.client.iterator.cursor.SpaceIteratorBatchResultProvider;
import com.gigaspaces.internal.client.SpaceIteratorBatchResult;
import com.gigaspaces.internal.space.requests.GetBatchForIteratorSpaceRequestInfo;
import com.gigaspaces.internal.space.requests.SpaceRequestInfo;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

/**
 * @author Alon Shoham
 * @since 15.2.0
 */
@com.gigaspaces.api.InternalApi
public class GetBatchForIteratorDistributedSpaceTask extends SystemDistributedTask<SpaceIteratorBatchResult> implements AsyncResultFilter<SpaceIteratorBatchResult> {
    private static final long serialVersionUID = 1L;
    private transient SpaceIteratorBatchResultProvider _spaceIteratorBatchResultProvider;
    private GetBatchForIteratorSpaceRequestInfo _getBatchForIteratorSpaceRequestInfo;

    public GetBatchForIteratorDistributedSpaceTask() {
    }

    public GetBatchForIteratorDistributedSpaceTask(SpaceIteratorBatchResultProvider spaceIteratorBatchResultProvider) {
        _spaceIteratorBatchResultProvider = spaceIteratorBatchResultProvider;
        _getBatchForIteratorSpaceRequestInfo = new GetBatchForIteratorSpaceRequestInfo(_spaceIteratorBatchResultProvider.getQueryPacket(), _spaceIteratorBatchResultProvider.getReadModifiers(), _spaceIteratorBatchResultProvider.getBatchSize(), 0, _spaceIteratorBatchResultProvider.getUuid(), spaceIteratorBatchResultProvider.getMaxInactiveDuration());
    }

    /*
    On result, call BatchManager.addResult
     */
    @Override
    public Decision onResult(AsyncResultFilterEvent<SpaceIteratorBatchResult> event) {
        //TODO add log message here
        _spaceIteratorBatchResultProvider.addAsyncBatchResult(event.getCurrentResult());
        return Decision.CONTINUE;
    }

    @Override
    public SpaceIteratorBatchResult reduce(List<AsyncResult<SpaceIteratorBatchResult>> asyncResults) throws Exception {
        throw new UnsupportedOperationException("Space iterator init task does not support reduce");
    }

    @Override
    public SpaceRequestInfo getSpaceRequestInfo() {
        return _getBatchForIteratorSpaceRequestInfo;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(_spaceIteratorBatchResultProvider);
        out.writeObject(_getBatchForIteratorSpaceRequestInfo);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        this._spaceIteratorBatchResultProvider = (SpaceIteratorBatchResultProvider) in.readObject();
        this._getBatchForIteratorSpaceRequestInfo = (GetBatchForIteratorSpaceRequestInfo) in.readObject();
    }
}
