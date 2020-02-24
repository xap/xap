package com.gigaspaces.internal.oshi;

import com.gigaspaces.CommonSystemProperties;
import oshi.SystemInfo;

import java.util.logging.Logger;

public class OshiChecker {

    private static final Logger logger = Logger.getLogger(OshiChecker.class.getName());
    private static final SystemInfo systemInfo = initSystemInfo();

    private static SystemInfo initSystemInfo() {
        String enabledProperty = System.getProperty(CommonSystemProperties.OSHI_ENABLED, "");
        boolean isImplicit = enabledProperty.isEmpty();
        boolean enabled = isImplicit || Boolean.parseBoolean(enabledProperty);
        if (!enabled) {
            logger.info("Oshi is disabled");
            return null;
        }

        if (isImplicit)
            logger.fine("Oshi is enabled");
        else
            logger.info("Oshi is enabled");

        try {
            SystemInfo systemInfo = new SystemInfo();
            if (isImplicit)
                logger.fine("Oshi is available");
            else
                logger.info("Oshi is available");
            return systemInfo;
        } catch (Throwable t) {
            if (isImplicit)
                logger.fine("Oshi is not available: " + t.toString());
            else
                logger.warning("Oshi is not available: " + t.toString());
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
