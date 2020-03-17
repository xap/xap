package com.gigaspaces.internal.server.space.executors;

import com.gigaspaces.internal.client.CloseIteratorSpaceResponseInfo;
import com.gigaspaces.internal.client.SpaceIteratorBatchResult;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.internal.server.space.iterator.ServerIteratorRequestInfo;
import com.gigaspaces.internal.space.requests.CloseIteratorSpaceRequestInfo;
import com.gigaspaces.internal.space.requests.GetBatchForIteratorSpaceRequestInfo;
import com.gigaspaces.internal.space.requests.SpaceRequestInfo;
import com.gigaspaces.internal.space.responses.SpaceResponseInfo;
import com.gigaspaces.security.authorities.SpaceAuthority;
import com.j_spaces.core.GetBatchForIteratorException;
import com.j_spaces.core.ServerIteratorAnswerHolder;

import java.util.UUID;

public class SpaceCloseIteratorExecutor extends SpaceActionExecutor{
    @Override
    public SpaceResponseInfo execute(SpaceImpl space, SpaceRequestInfo spaceRequestInfo) {
        CloseIteratorSpaceRequestInfo requestInfo = (CloseIteratorSpaceRequestInfo) spaceRequestInfo;
        space.closeServerIterator(requestInfo.getIteratorId());
        return new CloseIteratorSpaceResponseInfo();
    }

    @Override
    public SpaceAuthority.SpacePrivilege getPrivilege() {
        return SpaceAuthority.SpacePrivilege.READ;
    }
}
