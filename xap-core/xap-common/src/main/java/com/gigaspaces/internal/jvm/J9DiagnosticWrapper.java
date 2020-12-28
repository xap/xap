package com.gigaspaces.internal.jvm;



public class J9DiagnosticWrapper implements JVMDiagnosticWrapper {
    Class<?> dumpClass;

    public J9DiagnosticWrapper() throws ClassNotFoundException {
        this.dumpClass = Class.forName("com.ibm.jvm.Dump",
                true, J9DiagnosticWrapper.class.getClassLoader());
    }

    @Override
    public void dumpHeap(String outputFile, boolean live) {
        throw new UnsupportedOperationException("DumpHeap operation is unsupported with Java vendor: " + JavaUtils.getVendor());
    }

    @Override
    public boolean useCompressedOopsAsBoolean() {
        String vmInfo = System.getProperty("java.vm.info");
        return vmInfo != null && vmInfo.contains("Compressed References");
    }
}
