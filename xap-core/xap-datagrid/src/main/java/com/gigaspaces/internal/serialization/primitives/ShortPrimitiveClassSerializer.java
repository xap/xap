package com.gigaspaces.internal.serialization.primitives;

import com.gigaspaces.internal.serialization.IClassSerializer;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

@com.gigaspaces.api.InternalApi
public class ShortPrimitiveClassSerializer implements IClassSerializer<Short> {
    private static final Short DEFAULT_VALUE = 0;

    public byte getCode() {
        return CODE_SHORT;
    }

    public Short read(ObjectInput in)
            throws IOException, ClassNotFoundException {
        return in.readShort();
    }

    public void write(ObjectOutput out, Short obj)
            throws IOException {
        out.writeShort(obj.shortValue());
    }

    @Override
    public Short getDefaultValue() {
        return DEFAULT_VALUE;
    }
}
