package com.gigaspaces.logger;

import com.gigaspaces.CommonSystemProperties;
import com.gigaspaces.internal.utils.GsEnv;

import java.io.File;
import java.lang.management.ManagementFactory;

public class LoggerSystemInfo {

    public static final String xapHome = findXapHome();
    public static final long processId = findProcessId();

    private static String findXapHome() {
        String result = System.getProperty(CommonSystemProperties.GS_HOME);
        if (result == null)
            result = GsEnv.get("HOME", ".");

        result = trimSuffix(result, File.separator);
        System.setProperty(CommonSystemProperties.GS_HOME, result);
        return result;
    }

    private static long findProcessId() {
        String name = ManagementFactory.getRuntimeMXBean().getName();
        int pos = name.indexOf('@');
        if (pos < 1)
            return -1;
        try {
            return Long.parseLong(name.substring(0, pos));
        } catch (NumberFormatException e) {
            return -1;
        }
    }


    private static String trimSuffix(String s, String suffix) {
        return s.endsWith(suffix) ? s.substring(0, s.length() - suffix.length()) : s;
    }
}
