package com.gigaspaces.internal.serialization.primitives;

import com.gigaspaces.internal.serialization.IClassSerializer;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

@com.gigaspaces.api.InternalApi
public class IntPrimitiveClassSerializer implements IClassSerializer<Integer> {
    private static final Integer DEFAULT_VALUE = 0;

    public byte getCode() {
        return CODE_INTEGER;
    }

    public Integer read(ObjectInput in)
            throws IOException, ClassNotFoundException {
        return in.readInt();
    }

    public void write(ObjectOutput out, Integer obj)
            throws IOException {
        out.writeInt(obj);
    }

    @Override
    public Integer getDefaultValue() {
        return DEFAULT_VALUE;
    }
}
