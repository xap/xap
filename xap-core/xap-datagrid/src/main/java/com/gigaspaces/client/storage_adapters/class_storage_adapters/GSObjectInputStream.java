package com.gigaspaces.client.storage_adapters.class_storage_adapters;

import java.io.*;

public class GSObjectInputStream extends DataInputStream implements ObjectInput{
    private ObjectInputStream ois;

    public GSObjectInputStream(java.io.InputStream in) throws IOException {
        super(in);
        readByte();
    }

    public Object readObject() throws IOException, ClassNotFoundException {
        if (ois == null) {
            ois = new GSObjectInputStream.HeaderlessObjectInputStream(super.in);
        }
        return ois.readObject();
    }

    @Override
    public void close() throws IOException {
        if (ois != null)
            ois.close();
        super.close();
    }

    private static class HeaderlessObjectInputStream extends ObjectInputStream {

        public HeaderlessObjectInputStream(java.io.InputStream in) throws IOException {
            super(in);
        }

        @Override
        protected void readStreamHeader() throws IOException {
        }
    }
}
