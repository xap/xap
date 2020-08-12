package com.gigaspaces.internal.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;

public interface ObjectInputStreamFactory {
    ObjectInputStream create(InputStream is) throws IOException;

    public static class Default implements ObjectInputStreamFactory {
        public static final ObjectInputStreamFactory instance = new Default();

        @Override
        public ObjectInputStream create(InputStream is) throws IOException {
            return new ObjectInputStream(is);
        }
    }
}
