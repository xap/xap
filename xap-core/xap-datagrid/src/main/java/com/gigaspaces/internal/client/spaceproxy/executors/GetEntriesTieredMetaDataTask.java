package com.gigaspaces.internal.client.spaceproxy.executors;

import com.gigaspaces.async.AsyncResult;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.space.requests.GetEntriesTieredMetaDataRequestInfo;
import com.gigaspaces.internal.space.requests.SpaceRequestInfo;
import com.gigaspaces.internal.space.responses.GetEntriesTieredMetaDataResponseInfo;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

@com.gigaspaces.api.InternalApi
public class GetEntriesTieredMetaDataTask extends SystemDistributedTask<GetEntriesTieredMetaDataResponseInfo>{
    static final long serialVersionUID = -516199657333047744L;
    private GetEntriesTieredMetaDataRequestInfo _getEntriesTieredMetaDataRequestInfo;

    public GetEntriesTieredMetaDataTask() {
    }

    public GetEntriesTieredMetaDataTask(GetEntriesTieredMetaDataRequestInfo getEntriesTieredMetaDataRequestInfo) {
        this._getEntriesTieredMetaDataRequestInfo = getEntriesTieredMetaDataRequestInfo;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        IOUtils.writeObject(out, _getEntriesTieredMetaDataRequestInfo);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        this._getEntriesTieredMetaDataRequestInfo = IOUtils.readObject(in);
    }

    @Override
    public GetEntriesTieredMetaDataResponseInfo reduce(List<AsyncResult<GetEntriesTieredMetaDataResponseInfo>> asyncResults) throws Exception {
       return _getEntriesTieredMetaDataRequestInfo.reduce(asyncResults);
    }

    @Override
    public SpaceRequestInfo getSpaceRequestInfo() {
        return _getEntriesTieredMetaDataRequestInfo;
    }
}
