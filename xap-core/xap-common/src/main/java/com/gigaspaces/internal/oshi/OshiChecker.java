package com.gigaspaces.internal.oshi;

import com.gigaspaces.CommonSystemProperties;
import oshi.SystemInfo;
import java.util.logging.Logger;

public class OshiChecker {

    private static final Logger logger = Logger.getLogger(OshiChecker.class.getName());
    private static final boolean enabled = initEnabled();

    private static final SystemInfo systemInfo = initSystemInfo();

    private static boolean initEnabled() {
        String enabled = System.getProperty(CommonSystemProperties.OSHI_ENABLED, "");

        if(enabled.isEmpty() || Boolean.parseBoolean(enabled)){
            logger.info("Oshi is enabled");
            return true;
        }
        return false;
    }

    private static SystemInfo initSystemInfo() {
        try {
            return new SystemInfo();
        }catch (Throwable t) {
            logger.warning("Oshi is not available "+t.getMessage());
        }
        return null;
    }

    public static boolean isAvailable() {

        return enabled && (systemInfo != null);
    }

    public static SystemInfo getSystemInfo(){
        return systemInfo;
    }
}
