package com.gigaspaces.internal.serialization.primitives;

import com.gigaspaces.internal.serialization.IClassSerializer;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

@com.gigaspaces.api.InternalApi
public class CharPrimitiveClassSerializer implements IClassSerializer<Character> {
    private static final Character DEFAULT_VALUE = 	'\u0000';

    public byte getCode() {
        return CODE_CHARACTER;
    }

    public Character read(ObjectInput in)
            throws IOException, ClassNotFoundException {
        return in.readChar();
    }

    public void write(ObjectOutput out, Character obj)
            throws IOException {
        out.writeChar(obj.charValue());
    }

    @Override
    public Character getDefaultValue() {
        return DEFAULT_VALUE;
    }
}