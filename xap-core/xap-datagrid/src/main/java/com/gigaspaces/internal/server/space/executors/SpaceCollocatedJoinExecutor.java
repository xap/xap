/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
