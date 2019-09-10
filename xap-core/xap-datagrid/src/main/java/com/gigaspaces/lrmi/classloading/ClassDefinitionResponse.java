package com.gigaspaces.lrmi.classloading;

import java.io.Serializable;
/**
 * Response to a class definition request, holds the class bytes
 *
 * @author alon shoham
 * @since 15.0
 */

public class ClassDefinitionResponse implements Serializable {
    private static final long serialVersionUID = 1L;
    private final byte[] classBytes;
    private final Exception exception;

    public ClassDefinitionResponse() {
        classBytes = new byte[0];
        exception = null;
    }

    public ClassDefinitionResponse(byte[] classBytes, Exception e) {
        this.classBytes = classBytes;
        this.exception = e;
    }

    public byte[] getClassBytes() {
        return classBytes;
    }

    public Exception getException() {
        return exception;
    }
}
