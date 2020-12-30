package com.gigaspaces.internal.server.space.repartitioning;

public enum ScaleType {
    IN(0),
    OUT(1);
    public final byte value;

    ScaleType(int value) {
        this.value = (byte) value;
    }
}
