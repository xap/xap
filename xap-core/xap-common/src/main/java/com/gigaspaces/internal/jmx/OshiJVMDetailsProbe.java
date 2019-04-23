package com.gigaspaces.internal.jmx;

import com.gigaspaces.internal.jvm.JVMDetails;
import com.gigaspaces.internal.jvm.JVMDetailsProbe;
import com.gigaspaces.internal.oshi.OshiChecker;
import oshi.SystemInfo;
import oshi.software.os.OperatingSystem;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.RuntimeMXBean;
import java.util.UUID;

@com.gigaspaces.api.InternalApi
public class OshiJVMDetailsProbe  implements JVMDetailsProbe {
    private static final String uid;

    private static RuntimeMXBean runtimeMXBean;

    private static MemoryMXBean memoryMXBean;

    private static OperatingSystem oshiOperatingSystem;

    static {
        SystemInfo oshiSystemInfo = OshiChecker.getSystemInfo();
        oshiOperatingSystem = oshiSystemInfo.getOperatingSystem();
        uid =  UUID.randomUUID().toString();
        runtimeMXBean = ManagementFactory.getRuntimeMXBean();
        memoryMXBean = ManagementFactory.getMemoryMXBean();
    }

    public JVMDetails probeDetails() {
        return new JVMDetails(uid, runtimeMXBean.getVmName(),
                System.getProperty("java.version"),
                System.getProperty("java.vendor"),
                runtimeMXBean.getStartTime(),
                memoryMXBean.getHeapMemoryUsage().getInit(),
                memoryMXBean.getHeapMemoryUsage().getMax(),
                memoryMXBean.getNonHeapMemoryUsage().getInit(),
                memoryMXBean.getNonHeapMemoryUsage().getMax(),
                runtimeMXBean.getInputArguments().toArray(new String[0]),
                runtimeMXBean.isBootClassPathSupported() ? runtimeMXBean.getBootClassPath() : "",
                runtimeMXBean.getClassPath(),
                runtimeMXBean.getSystemProperties(),
                System.getenv(),
                oshiOperatingSystem.getProcessId());
    }
}
