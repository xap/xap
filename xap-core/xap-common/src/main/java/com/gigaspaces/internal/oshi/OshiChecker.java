package com.gigaspaces.internal.oshi;

import com.gigaspaces.CommonSystemProperties;
import oshi.SystemInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import oshi.hardware.HardwareAbstractionLayer;
import oshi.software.os.OSProcess;
import oshi.software.os.OperatingSystem;

public class OshiChecker {

    private static final Logger logger = LoggerFactory.getLogger(OshiChecker.class.getName());
    private static final SystemInfo systemInfo = initSystemInfo();
    private static final OperatingSystem operatingSystem = systemInfo != null ? systemInfo.getOperatingSystem() : null;
    private static final HardwareAbstractionLayer hardware = systemInfo != null ? systemInfo.getHardware() : null;

    private static SystemInfo initSystemInfo() {
        String enabledProperty = System.getProperty(CommonSystemProperties.OSHI_ENABLED, "");
        boolean isImplicit = enabledProperty.isEmpty();
        boolean enabled = isImplicit || Boolean.parseBoolean(enabledProperty);
        if (!enabled) {
            logger.info("Oshi is disabled");
            return null;
        }

        if (isImplicit)
            logger.debug("Oshi is enabled");
        else
            logger.info("Oshi is enabled");

        try {
            SystemInfo systemInfo = new SystemInfo();
            if (isImplicit)
                logger.debug("Oshi is available");
            else
                logger.info("Oshi is available");
            return systemInfo;
        } catch (Throwable t) {
            if (isImplicit)
                logger.debug("Oshi is not available: " + t.toString());
            else
                logger.warn("Oshi is not available: " + t.toString());
            return null;
        }
    }

    public static boolean isAvailable() {
        return systemInfo != null;
    }

    public static OperatingSystem getOperatingSystem() {
        return operatingSystem;
    }

    public static OSProcess getProcessInfo(long pid) {
        return operatingSystem.getProcess((int) pid);
    }

    public static HardwareAbstractionLayer getHardware() {
        return hardware;
    }
}
