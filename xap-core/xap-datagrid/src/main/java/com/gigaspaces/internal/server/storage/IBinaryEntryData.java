package com.gigaspaces.internal.server.storage;

public interface IBinaryEntryData extends ITransactionalEntryData {
    byte[] getSerializedFields();

    boolean isEqualProperties(IBinaryEntryData old);
}
