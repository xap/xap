package com.gigaspaces.internal.os.oshi;

import com.gigaspaces.internal.jvm.JavaUtils;
import com.gigaspaces.internal.os.ProcessCpuSampler;
import com.gigaspaces.internal.oshi.OshiUtils;
import oshi.software.os.OSProcess;

/**
 * @author Niv Ingberg
 * @since 15.5.1
 */
public class OshiProcessCpuSampler implements ProcessCpuSampler {
    private final int pid = (int) JavaUtils.getPid();

    @Override
    public long sampleTotalCpuTime() {
        OSProcess process = OshiUtils.getOperatingSystem().getProcess(pid);
        return process.getKernelTime() + process.getUserTime();
    }
}
