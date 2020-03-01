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

package com.gigaspaces.internal.server.space.operations;

import com.gigaspaces.internal.client.spaceproxy.executors.SystemTask;
import com.gigaspaces.internal.client.spaceproxy.operations.ExecuteTaskSpaceOperationRequest;
import com.gigaspaces.internal.client.spaceproxy.operations.ExecuteTaskSpaceOperationResult;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.security.authorities.SpaceAuthority;

/**
 * @author Niv Ingberg
 * @since 9.0.0
 */
@com.gigaspaces.api.InternalApi
public class ExecuteTaskSpaceOperation extends AbstractSpaceOperation<ExecuteTaskSpaceOperationResult, ExecuteTaskSpaceOperationRequest> {
    @Override
    public void execute(ExecuteTaskSpaceOperationRequest request, ExecuteTaskSpaceOperationResult result, SpaceImpl space, boolean oneway)
            throws Exception {
        Object taskResult = space.executeTask(request.getTask(), request.getTransaction(), request.getSpaceContext(), true);
        result.setResult(taskResult);
    }

    @Override
    public String getLogName(ExecuteTaskSpaceOperationRequest request, ExecuteTaskSpaceOperationResult result) {
        return "execute task";
    }
}
