package com.gigaspaces.internal.server.space.tiered_storage.error;

public class TieredStorageConfigException extends RuntimeException {
    static final long serialVersionUID = 9140573569012921556L;
    public TieredStorageConfigException(String message) {
        super(message);
    }
}
