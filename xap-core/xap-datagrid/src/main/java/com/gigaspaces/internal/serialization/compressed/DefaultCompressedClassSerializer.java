package com.gigaspaces.internal.serialization.compressed;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.io.PooledObjectConverter;
import com.gigaspaces.internal.serialization.IClassSerializer;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author Niv Ingberg
 * @since 15.8
 */
@com.gigaspaces.api.InternalApi
public class DefaultCompressedClassSerializer implements IClassSerializer<Object> {
    public static final DefaultCompressedClassSerializer instance = new DefaultCompressedClassSerializer();

    @Override
    public byte getCode() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void write(ObjectOutput out, Object obj) throws IOException {
        byte[] bytes = PooledObjectConverter.zip(obj);
        IOUtils.writeInt(out, bytes.length);
        out.write(bytes);
    }

    @Override
    public Object read(ObjectInput in) throws IOException, ClassNotFoundException {
        int length = IOUtils.readInt(in);
        byte[] bytes = new byte[length];
        in.read(bytes);
        return PooledObjectConverter.unzip(bytes);
    }
}
