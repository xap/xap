package com.gigaspaces.sync.serializable;

import com.gigaspaces.api.InternalApi;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.sync.IntroduceTypeData;

@InternalApi
public class IntroduceTypeEndpointData implements IntroduceTypeData, EndpointData {

    private static final long serialVersionUID = 258825999388261637L;

    private final ITypeDesc spaceTypeDescritor;
    private final SpaceSyncEndpointMethod spaceSyncEndpointMethod;

    public IntroduceTypeEndpointData(com.gigaspaces.sync.IntroduceTypeData introduceTypeData, SpaceSyncEndpointMethod spaceSyncEndpointMethod) {
        this.spaceTypeDescritor = (ITypeDesc) introduceTypeData.getTypeDescriptor();
        this.spaceSyncEndpointMethod = spaceSyncEndpointMethod;
    }

    @Override
    public SpaceTypeDescriptor getTypeDescriptor() {
        return spaceTypeDescritor;
    }

    @Override
    public SpaceSyncEndpointMethod getSyncEndpointMethod() {
        return spaceSyncEndpointMethod;
    }
}
