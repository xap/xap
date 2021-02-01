package com.gigaspaces.internal.server.space.executors;

import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.internal.space.requests.GetEntriesTieredMetaDataRequestInfo;
import com.gigaspaces.internal.space.requests.SpaceRequestInfo;
import com.gigaspaces.internal.space.responses.GetEntriesTieredMetaDataResponseInfo;
import com.gigaspaces.internal.space.responses.SpaceResponseInfo;

@com.gigaspaces.api.InternalApi
public class SpaceGetEntriesTieredMetaDataExecutor extends SpaceActionExecutor{
    @Override
    public SpaceResponseInfo execute(SpaceImpl space, SpaceRequestInfo spaceRequestInfo) {
        GetEntriesTieredMetaDataResponseInfo responseInfo = new GetEntriesTieredMetaDataResponseInfo();
        GetEntriesTieredMetaDataRequestInfo requestInfo = (GetEntriesTieredMetaDataRequestInfo) spaceRequestInfo;
        try {
            responseInfo.setEntryMetaDataMap(space.getEntriesTieredMetaDataByIds(requestInfo.getTypeName(), requestInfo.getObjectsIds()));
        } catch (Exception e) {
            responseInfo.addException(space.getPartitionId(), e);
        }
        return responseInfo;
    }
}
