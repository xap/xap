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

    public ClassDefinitionResponse() {
        classBytes = new byte[0];
    }

    public ClassDefinitionResponse(byte[] classBytes) {
        this.classBytes = classBytes;
    }

    public byte[] getClassBytes() {
        return classBytes;
    }
}
