package com.gigaspaces.internal.exceptions;

public class ChunksMapMissingException extends Exception{
    public ChunksMapMissingException() {
        super();
    }

    public ChunksMapMissingException(String message) {
        super(message);
    }

    public ChunksMapMissingException(Exception e) {
        super(e);
    }
}
