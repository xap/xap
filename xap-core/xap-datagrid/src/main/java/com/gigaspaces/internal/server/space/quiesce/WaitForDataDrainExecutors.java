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
package com.gigaspaces.internal.server.space.quiesce;

import com.gigaspaces.internal.extension.WaitForDataDrainRequest;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.internal.server.space.executors.SpaceActionExecutor;
import com.gigaspaces.internal.space.requests.SpaceRequestInfo;
import com.gigaspaces.internal.space.responses.SpaceResponseInfo;

import java.util.concurrent.TimeoutException;

public class WaitForDataDrainExecutors extends SpaceActionExecutor {

    @Override
    public SpaceResponseInfo execute(SpaceImpl space, SpaceRequestInfo spaceRequestInfo) {
        final WaitForDataDrainRequest drainRequest = (WaitForDataDrainRequest) spaceRequestInfo;
        final WaitForDrainPartitionResponse response = new WaitForDrainPartitionResponse(space.getPartitionIdOneBased());
        try {
            space.waitForDrain(drainRequest.getTimeout(), drainRequest.getMinTimeToWait(), drainRequest.isDemote(), null);
            response.setSuccessful(true);
        } catch (TimeoutException e) {
            response.setException(e);
        }

        return response;
    }
}
