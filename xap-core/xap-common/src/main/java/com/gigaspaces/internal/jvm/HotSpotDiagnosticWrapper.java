package com.gigaspaces.internal.jvm;

import com.sun.management.HotSpotDiagnosticMXBean;
import com.sun.management.VMOption;


import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.management.ManagementFactory;


public class HotSpotDiagnosticWrapper implements JVMDiagnosticWrapper {
    HotSpotDiagnosticMXBean bean;

    public HotSpotDiagnosticWrapper() {
        try {
        bean = ManagementFactory.newPlatformMXBeanProxy(ManagementFactory.getPlatformMBeanServer(),
                    "com.sun.management:type=HotSpotDiagnostic", HotSpotDiagnosticMXBean.class);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void dumpHeap(String outputFile, boolean live) throws IOException {
        bean.dumpHeap(outputFile, live);
    }

    @Override
    public boolean useCompressedOopsAsBoolean() {
       VMOption vmOption = getVMOption("UseCompressedOops");
       String val = vmOption != null ? vmOption.getValue(): null;
       return Boolean.parseBoolean(val);
    }

    public VMOption getVMOption(String name) {
       return bean.getVMOption(name);
    }
}
