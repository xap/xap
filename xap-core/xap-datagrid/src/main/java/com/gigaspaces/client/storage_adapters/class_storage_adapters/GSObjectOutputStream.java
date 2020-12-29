package com.gigaspaces.client.storage_adapters.class_storage_adapters;

import java.io.*;


public class GSObjectOutputStream extends DataOutputStream implements ObjectOutput {
    private ObjectOutputStream oos;

    public GSObjectOutputStream(java.io.OutputStream out) {
        super(out);
    }

    public void writeObject(Object obj) throws IOException {
        if (oos == null) {
            oos = new HeaderlessObjectOutputStream(super.out);
        }
        oos.writeObject(obj);
    }

    @Override
    public void close() throws IOException {
        if (oos != null)
            oos.close();
        super.close();
    }

    private static class HeaderlessObjectOutputStream extends ObjectOutputStream {

        public HeaderlessObjectOutputStream(java.io.OutputStream out) throws IOException {
            super(out);
        }

        @Override
        protected void writeStreamHeader() throws IOException {
            // skip header to reduce footprint super.writeStreamHeader();
        }
    }
}




















































