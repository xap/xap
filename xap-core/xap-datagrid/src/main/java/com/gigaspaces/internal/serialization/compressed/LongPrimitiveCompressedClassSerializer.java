package com.gigaspaces.internal.serialization.compressed;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.serialization.IClassSerializer;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author Niv Ingberg
 * @since 15.8
 */
@com.gigaspaces.api.InternalApi
public class LongPrimitiveCompressedClassSerializer implements IClassSerializer<Long> {
    private static final Long DEFAULT_VALUE = 0L;

    @Override
    public byte getCode() {
        return CODE_LONG;
    }

    @Override
    public void write(ObjectOutput out, Long obj) throws IOException {
        IOUtils.writeLong(out, obj);
    }

    @Override
    public Long read(ObjectInput in) throws IOException, ClassNotFoundException {
        return IOUtils.readLong(in);
    }

    @Override
    public Long getDefaultValue() {
        return DEFAULT_VALUE;
    }
}
