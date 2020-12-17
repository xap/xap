package com.gigaspaces.internal.server.storage;

public interface IBinaryEntryData extends ITransactionalEntryData {
    byte[] getPackedSerializedProperties();

    boolean isEqualProperties(IBinaryEntryData old);
}
