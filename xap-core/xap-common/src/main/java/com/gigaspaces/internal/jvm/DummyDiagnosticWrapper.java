package com.gigaspaces.internal.jvm;


public class DummyDiagnosticWrapper implements JVMDiagnosticWrapper{
    @Override
    public void dumpHeap(String outputFile, boolean live) {
        throw new UnsupportedOperationException("DumpHeap operation is unsupported with Java vendor: " + JavaUtils.getVendor());
    }

    @Override
    public boolean useCompressedOopsAsBoolean() {
        return true;
    }
}
