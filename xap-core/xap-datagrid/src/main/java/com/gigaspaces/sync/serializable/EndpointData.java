package com.gigaspaces.sync.serializable;

import com.gigaspaces.api.InternalApi;
import com.gigaspaces.sync.DataSyncOperation;

import java.io.Serializable;
import java.util.Arrays;

@InternalApi
public interface EndpointData extends Serializable {
    SpaceSyncEndpointMethod getSyncEndpointMethod();
}
