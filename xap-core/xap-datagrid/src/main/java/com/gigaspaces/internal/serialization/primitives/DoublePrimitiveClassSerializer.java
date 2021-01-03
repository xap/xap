package com.gigaspaces.internal.serialization.primitives;

import com.gigaspaces.internal.serialization.IClassSerializer;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

@com.gigaspaces.api.InternalApi
public class DoublePrimitiveClassSerializer implements IClassSerializer<Double> {
    private static final Double DEFAULT_VALUE = 0.0d;

    public byte getCode() {
        return CODE_DOUBLE;
    }

    public Double read(ObjectInput in)
            throws IOException, ClassNotFoundException {
        return in.readDouble();
    }


    public void write(ObjectOutput out, Double obj)
            throws IOException {
        out.writeDouble(obj.doubleValue());
    }

    @Override
    public Double getDefaultValue() {
        return DEFAULT_VALUE;
    }
}
