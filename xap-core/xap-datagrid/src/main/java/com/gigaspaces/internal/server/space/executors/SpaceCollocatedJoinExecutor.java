package com.gigaspaces.internal.server.space.executors;

import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.internal.space.requests.CollocatedJoinSpaceRequestInfo;
import com.gigaspaces.internal.space.requests.SpaceRequestInfo;
import com.gigaspaces.internal.space.responses.CollocatedJoinSpaceResponseInfo;
import com.gigaspaces.internal.space.responses.SpaceResponseInfo;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.security.authorities.SpaceAuthority;
import com.j_spaces.jdbc.executor.JoinedQueryExecutor;
import com.j_spaces.jdbc.query.IQueryResultSet;
import com.j_spaces.jdbc.query.JoinedQueryResult;

public class SpaceCollocatedJoinExecutor extends SpaceActionExecutor {
    @Override
    public SpaceResponseInfo execute(SpaceImpl space, SpaceRequestInfo spaceRequestInfo) {
        CollocatedJoinSpaceRequestInfo requestInfo = (CollocatedJoinSpaceRequestInfo) spaceRequestInfo;


        JoinedQueryExecutor executor = new JoinedQueryExecutor(requestInfo.getQuery());
        try {
            IQueryResultSet<IEntryPacket> res = executor.execute(space.getSingleProxy(), requestInfo.getTxn(), requestInfo.getReadModifier(), requestInfo.getMax());
            return new CollocatedJoinSpaceResponseInfo(new JoinedQueryResult(res));
        } catch (Exception e) {
            throw new RuntimeException("Failed to execute join", e);
        }

    }

    @Override
    public SpaceAuthority.SpacePrivilege getPrivilege() {
        return SpaceAuthority.SpacePrivilege.READ;
    }
}
