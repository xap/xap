package com.gigaspaces.internal.jvm;


public interface JVMDiagnosticWrapper {

    void dumpHeap(String outputFile, boolean live) throws java.io.IOException;

    boolean useCompressedOopsAsBoolean();
}



