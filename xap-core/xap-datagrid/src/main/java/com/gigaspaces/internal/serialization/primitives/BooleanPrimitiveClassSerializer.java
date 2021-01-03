package com.gigaspaces.internal.serialization.primitives;

import com.gigaspaces.internal.serialization.IClassSerializer;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

@com.gigaspaces.api.InternalApi
public class BooleanPrimitiveClassSerializer implements IClassSerializer<Boolean> {
    private static final Boolean DEFAULT_VALUE = false;

    public byte getCode() {
        return CODE_BOOLEAN;
    }

    public Boolean read(ObjectInput in)
            throws IOException, ClassNotFoundException {
        return in.readBoolean();
    }

    public void write(ObjectOutput out, Boolean obj)
            throws IOException {
        out.writeBoolean(obj.booleanValue());
    }

    @Override
    public Boolean getDefaultValue() {
        return DEFAULT_VALUE;
    }
}
