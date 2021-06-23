package com.gigaspaces.internal.space.requests;

import com.gigaspaces.async.AsyncResult;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.space.responses.GetEntriesTieredMetaDataResponseInfo;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

@com.gigaspaces.api.InternalApi
public class GetEntriesTieredMetaDataRequestInfo extends AbstractSpaceRequestInfo{
    static final long serialVersionUID = 7159851750091653967L;
    private String typeName;
    private Object[] objectsIds;

    public GetEntriesTieredMetaDataRequestInfo() { }

    public GetEntriesTieredMetaDataRequestInfo(String typeName, Object[] objectsIds) {
        this.typeName = typeName;
        this.objectsIds = objectsIds;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeString(out, typeName);
        IOUtils.writeObjectArray(out, objectsIds);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException{
        this.typeName = IOUtils.readString(in);
        this.objectsIds = IOUtils.readObjectArray(in);
    }

    public Object[] getObjectsIds() {
        return objectsIds;
    }

    public void setObjectsIds(Object[] objectsIds) {
        this.objectsIds = objectsIds;
    }

    public String getTypeName() {
        return typeName;
    }

    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }

    public GetEntriesTieredMetaDataResponseInfo reduce(List<AsyncResult<GetEntriesTieredMetaDataResponseInfo>> asyncResults) throws Exception {
        GetEntriesTieredMetaDataResponseInfo result = new GetEntriesTieredMetaDataResponseInfo();
        for (AsyncResult<GetEntriesTieredMetaDataResponseInfo> asyncResult : asyncResults){
            if (asyncResult.getException() != null) {
                throw asyncResult.getException();
            }
            GetEntriesTieredMetaDataResponseInfo responseInfo = asyncResult.getResult();
            result.getExceptionMap().putAll(responseInfo.getExceptionMap());
            if(responseInfo.getEntryMetaDataMap() != null){
                result.addResultToEntryMetaDataMap(responseInfo.getEntryMetaDataMap());
            }
        }
        return result;
    }
}
