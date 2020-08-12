package com.gigaspaces.sync.serializable;

import com.gigaspaces.api.InternalApi;

@InternalApi
public enum SpaceSyncEndpointMethod {
    onTransactionConsolidationFailure,
    onTransactionSynchronization,
    onOperationsBatchSynchronization,
    onAddIndex,
    onIntroduceType
}
