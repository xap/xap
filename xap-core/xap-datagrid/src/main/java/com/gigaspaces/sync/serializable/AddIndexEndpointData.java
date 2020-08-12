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
