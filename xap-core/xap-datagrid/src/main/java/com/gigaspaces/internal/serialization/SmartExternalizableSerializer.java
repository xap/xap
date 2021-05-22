package com.gigaspaces.internal.serialization;

import com.gigaspaces.internal.io.MarshalInputStream;
import com.gigaspaces.internal.io.MarshalOutputStream;
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author Niv Ingberg
 * @since 16.0
 */
public class SmartExternalizableSerializer<T extends Externalizable> implements IClassSerializer<T> {

    public static final SmartExternalizableSerializer instance = new SmartExternalizableSerializer();

    private SmartExternalizableSerializer() {
    }

    @Override
    public byte getCode() {
        return CODE_SMART_EXTERNALIZABLE;
    }

    @Override
    public void write(ObjectOutput out, T obj) throws IOException {
        ((MarshalOutputStream)out).writeSmartExternalizable((SmartExternalizable) obj);
    }

    @Override
    public T read(ObjectInput in) throws IOException, ClassNotFoundException {
        return (T) ((MarshalInputStream)in).readSmartExternalizable();
    }
}
