package com.gigaspaces.internal.client.spaceproxy.executors;

import com.gigaspaces.async.AsyncResult;
import com.gigaspaces.datasource.SpaceDataSourceLoadRequest;
import com.gigaspaces.datasource.SpaceDataSourceLoadResult;
import com.gigaspaces.executor.DistributedSpaceTask;
import com.gigaspaces.internal.space.requests.DataSourceLoadSpaceRequestInfo;
import com.gigaspaces.internal.space.responses.DataSourceLoadSpaceResponseInfo;
import com.gigaspaces.internal.space.requests.SpaceRequestInfo;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

/**
 * @author Alon Shoham
 * @since 15.5.0
 */
public class SpaceDataSourceLoadTask extends SystemTask<DataSourceLoadSpaceResponseInfo> implements DistributedSpaceTask<DataSourceLoadSpaceResponseInfo, SpaceDataSourceLoadResult> {
    private static final long serialVersionUID = 6291008493339604516L;
    private DataSourceLoadSpaceRequestInfo _dataSourceLoadSpaceRequestInfo;

    public SpaceDataSourceLoadTask() {
    }

    public SpaceDataSourceLoadTask(SpaceDataSourceLoadRequest spaceDataSourceLoadRequest) {
        this._dataSourceLoadSpaceRequestInfo = new DataSourceLoadSpaceRequestInfo(spaceDataSourceLoadRequest.getFactory(), spaceDataSourceLoadRequest.getAdaptersMap());
    }

    @Override
    public SpaceDataSourceLoadResult reduce(List<AsyncResult<DataSourceLoadSpaceResponseInfo>> asyncResults) throws Exception {
        return _dataSourceLoadSpaceRequestInfo.reduce(asyncResults);
    }

    @Override
    public SpaceRequestInfo getSpaceRequestInfo() {
        return _dataSourceLoadSpaceRequestInfo;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(_dataSourceLoadSpaceRequestInfo);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        _dataSourceLoadSpaceRequestInfo = (DataSourceLoadSpaceRequestInfo) in.readObject();
    }
}
