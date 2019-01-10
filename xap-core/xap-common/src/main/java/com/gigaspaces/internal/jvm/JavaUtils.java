package com.gigaspaces.internal.jvm;

import com.gigaspaces.api.InternalApi;

@InternalApi
public class JavaUtils {
    private static final int JAVA_VERSION_MAJOR = parseJavaMajorVersion();

    /**
     * Starting Java 9, format is: MAJOR.MINOR.SECURITY, where trailing 0 are omitted.
     * For example: 9, 9.0.1, etc.
     * Older versions format is 1.MAJOR.suffix
     * For example: 1.8.144, 1.7, etc.
     */
    private static int parseJavaMajorVersion() {
        String version = System.getProperty("java.version");
        // Remove "1." if exists (versions before 9):
        if (version.startsWith("1."))
            version = version.substring(2);
        // Find and remove everything after major, if any:
        int pos = version.indexOf('.');
        if (pos != -1)
            version = version.substring(0, pos);
        return Integer.parseInt(version);
    }

    public static int getMajorJavaVersion() {
        return JAVA_VERSION_MAJOR;
    }

    public static boolean greaterOrEquals(int version) {
        return JAVA_VERSION_MAJOR >= version;
    }
}
