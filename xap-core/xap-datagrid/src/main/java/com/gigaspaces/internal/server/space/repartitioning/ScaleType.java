package com.gigaspaces.internal.server.space.repartitioning;

/**
 * @author alon shoham
 * @since 15.8.0
 */
@com.gigaspaces.api.InternalApi
public enum ScaleType {
    IN(0),
    OUT(1);
    public final byte value;

    ScaleType(int value) {
        this.value = (byte) value;
    }
}
