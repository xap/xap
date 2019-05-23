package com.gigaspaces.internal.oshi;

import com.gigaspaces.CommonSystemProperties;
import oshi.SystemInfo;

import java.util.logging.Level;
import java.util.logging.Logger;

public class OshiChecker {

    private static final Logger logger = Logger.getLogger(OshiChecker.class.getName());
    private static final SystemInfo systemInfo = initSystemInfo();

    private static SystemInfo initSystemInfo() {
        String enabledProperty = System.getProperty(CommonSystemProperties.OSHI_ENABLED, "");
        boolean enabled = enabledProperty.isEmpty() || Boolean.parseBoolean(enabledProperty);
        if (!enabled) {
            logger.info("Oshi is disabled");
            return null;
        }

        logger.log(enabledProperty.isEmpty() ? Level.FINE : Level.INFO, "Oshi is enabled");

        try {
            SystemInfo systemInfo = new SystemInfo();
            logger.log(enabledProperty.isEmpty() ? Level.FINE : Level.INFO, "Oshi is available");
            return systemInfo;
        } catch (Throwable t) {
            logger.log(enabledProperty.isEmpty() ? Level.FINE : Level.WARNING, "Oshi is not available: " + t.toString());
            return null;
        }
    }

    public static boolean isAvailable() {
        return systemInfo != null;
    }

    public static SystemInfo getSystemInfo(){
        return systemInfo;
    }
}
