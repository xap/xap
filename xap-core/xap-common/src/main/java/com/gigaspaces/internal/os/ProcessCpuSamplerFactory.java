package com.gigaspaces.internal.os;

import com.gigaspaces.internal.jvm.JavaUtils;
import com.gigaspaces.internal.os.oshi.OshiProcessCpuSampler;
import com.gigaspaces.internal.oshi.OshiChecker;

public class ProcessCpuSamplerFactory {
    public static ProcessCpuSampler create() {
        if (JavaUtils.greaterOrEquals(11))
            return new JavaProcessCpuSampler();
        if (OshiChecker.isAvailable())
            return new OshiProcessCpuSampler();
        return () -> ProcessCpuSampler.NA;
    }
}
