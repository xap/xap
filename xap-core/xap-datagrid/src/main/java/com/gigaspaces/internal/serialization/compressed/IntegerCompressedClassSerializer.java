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
public class IntegerCompressedClassSerializer implements IClassSerializer<Integer> {
    @Override
    public byte getCode() {
        return CODE_INTEGER;
    }

    @Override
    public void write(ObjectOutput out, Integer obj) throws IOException {
        IOUtils.writeInt(out, obj);
    }

    @Override
    public Integer read(ObjectInput in) throws IOException, ClassNotFoundException {
        return IOUtils.readInt(in);
    }
}
