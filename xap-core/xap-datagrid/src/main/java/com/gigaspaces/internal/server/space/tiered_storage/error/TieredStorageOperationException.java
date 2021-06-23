package com.gigaspaces.internal.server.space.tiered_storage.error;

public class TieredStorageOperationException extends RuntimeException {
    static final long serialVersionUID = -7517727645228649286L;
    public TieredStorageOperationException(String message) {
        super(message);
    }
}
