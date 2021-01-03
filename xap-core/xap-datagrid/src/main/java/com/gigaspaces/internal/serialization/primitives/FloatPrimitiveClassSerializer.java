package com.gigaspaces.internal.serialization.primitives;

import com.gigaspaces.internal.serialization.IClassSerializer;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

@com.gigaspaces.api.InternalApi
public class FloatPrimitiveClassSerializer implements IClassSerializer<Float> {
    private static final Float DEFAULT_VALUE = 0.0f;

    public byte getCode() {
        return CODE_FLOAT;
    }

    public Float read(ObjectInput in)
            throws IOException, ClassNotFoundException {
        return in.readFloat();
    }

    public void write(ObjectOutput out, Float obj)
            throws IOException {
        out.writeFloat(obj.floatValue());
    }

    @Override
    public Float getDefaultValue() {
        return DEFAULT_VALUE;
    }
}
