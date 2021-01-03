package com.gigaspaces.internal.serialization.primitives;

import com.gigaspaces.internal.serialization.IClassSerializer;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

@com.gigaspaces.api.InternalApi
public class BytePrimitiveClassSerializer  implements IClassSerializer<Byte> {
    private static final Byte DEFAULT_VALUE = 0;

    public byte getCode() {
        return CODE_BYTE;
    }

    public Byte read(ObjectInput in)
            throws IOException, ClassNotFoundException {
        return in.readByte();
    }

    public void write(ObjectOutput out, Byte obj)
            throws IOException {
        out.writeByte(obj.byteValue());
    }

    @Override
    public Byte getDefaultValue() {
        return DEFAULT_VALUE;
    }
}
