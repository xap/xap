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

import com.gigaspaces.internal.client.CloseIteratorSpaceResponseInfo;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.internal.space.requests.CloseIteratorSpaceRequestInfo;
import com.gigaspaces.internal.space.requests.SpaceRequestInfo;
import com.gigaspaces.internal.space.responses.SpaceResponseInfo;
import com.gigaspaces.security.authorities.SpaceAuthority;

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
