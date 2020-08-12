package com.gigaspaces.internal.server.space.executors;

import com.gigaspaces.internal.client.RenewIteratorLeaseSpaceResponseInfo;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.internal.space.requests.RenewIteratorLeaseSpaceRequestInfo;
import com.gigaspaces.internal.space.requests.SpaceRequestInfo;
import com.gigaspaces.internal.space.responses.SpaceResponseInfo;
import com.gigaspaces.security.authorities.SpaceAuthority;

public class SpaceRenewIteratorLeaseExecutor extends SpaceActionExecutor{
    @Override
    public SpaceResponseInfo execute(SpaceImpl space, SpaceRequestInfo spaceRequestInfo) {
        RenewIteratorLeaseSpaceRequestInfo requestInfo = (RenewIteratorLeaseSpaceRequestInfo) spaceRequestInfo;
        space.renewServerIteratorLease(requestInfo.getIteratorId());
        return new RenewIteratorLeaseSpaceResponseInfo();
    }

    @Override
    public SpaceAuthority.SpacePrivilege getPrivilege() {
        return SpaceAuthority.SpacePrivilege.READ;
    }
}
