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

import com.gigaspaces.internal.client.spaceproxy.operations.CloseIteratorSpaceOperationRequest;
import com.gigaspaces.internal.client.spaceproxy.operations.CloseIteratorSpaceOperationResult;
import com.gigaspaces.internal.server.space.SpaceImpl;

/**
 * @author Alon Shoham
 * @since 15.2.0
 */
@com.gigaspaces.api.InternalApi
public class CloseIteratorSpaceOperation extends AbstractSpaceOperation<CloseIteratorSpaceOperationResult, CloseIteratorSpaceOperationRequest> {
    @Override
    public void execute(CloseIteratorSpaceOperationRequest request, CloseIteratorSpaceOperationResult result, SpaceImpl space, boolean oneway) throws Exception {
        space.closeServerIterator(request.getUuid());
    }

    @Override
    public String getLogName(CloseIteratorSpaceOperationRequest request, CloseIteratorSpaceOperationResult result) {
        return "close server iterator";
    }
}
