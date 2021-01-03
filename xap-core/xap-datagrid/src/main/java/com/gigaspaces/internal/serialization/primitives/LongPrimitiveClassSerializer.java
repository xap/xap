package com.gigaspaces.internal.serialization.primitives;

import com.gigaspaces.internal.serialization.IClassSerializer;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

@com.gigaspaces.api.InternalApi
public class LongPrimitiveClassSerializer implements IClassSerializer<Long> {
    private static final Long DEFAULT_VALUE = 0L;

    public byte getCode() {
        return CODE_LONG;
    }

    public Long read(ObjectInput in)
            throws IOException, ClassNotFoundException {
        return in.readLong();
    }

    public void write(ObjectOutput out, Long obj)
            throws IOException {
        out.writeLong(obj.longValue());
    }

    @Override
    public Long getDefaultValue() {
        return DEFAULT_VALUE;
    }
}
