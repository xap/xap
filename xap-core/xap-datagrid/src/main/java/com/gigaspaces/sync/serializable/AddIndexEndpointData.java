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
package com.gigaspaces.sync.serializable;

import com.gigaspaces.api.InternalApi;
import com.gigaspaces.metadata.index.ISpaceIndex;
import com.gigaspaces.metadata.index.SpaceIndex;
import com.gigaspaces.sync.AddIndexData;

@InternalApi
public class AddIndexEndpointData implements AddIndexData, EndpointData {

    private static final long serialVersionUID = 258825999388261637L;

    private final String typeName;
    private final ISpaceIndex[] spaceIndices;
    private final SpaceSyncEndpointMethod spaceSyncEndpointMethod;

    public AddIndexEndpointData(com.gigaspaces.sync.AddIndexData addIndexData, SpaceSyncEndpointMethod spaceSyncEndpointMethod) {
        this.typeName = addIndexData.getTypeName();
        this.spaceIndices = (ISpaceIndex[]) addIndexData.getIndexes();
        this.spaceSyncEndpointMethod = spaceSyncEndpointMethod;
    }

    @Override
    public String getTypeName() {
        return typeName;
    }

    @Override
    public SpaceIndex[] getIndexes() {
        return spaceIndices;
    }

    @Override
    public SpaceSyncEndpointMethod getSyncEndpointMethod() {
        return spaceSyncEndpointMethod;
    }
}
