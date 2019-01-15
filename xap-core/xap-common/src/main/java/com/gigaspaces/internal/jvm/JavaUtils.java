package com.gigaspaces.internal.jvm;

import com.gigaspaces.api.InternalApi;

@InternalApi
public class JavaUtils {
    private static final String VENDOR = initVendor();
    private static final int JAVA_VERSION_MAJOR = parseJavaMajorVersion();
    private static final boolean isWindows = System.getProperty("os.name").toLowerCase().startsWith("win");
    private static final boolean isOsx = System.getProperty("os.name").toLowerCase().startsWith("mac os x");

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

    public static boolean isWindows() {
        return isWindows;
    }

    public static boolean isOsx() {
        return isOsx;
    }

    public static void main(String[] args) {
        try {
            String request = args.length != 0 ? args[0].toLowerCase() : "version";
            switch (request) {
                case "version":
                    System.out.println(JavaUtils.getMajorJavaVersion());
                    break;
                default:
                    System.out.println("Unsupported: [" + request + "]");
                    break;
            }
        } catch (Exception e) {
            System.out.println("Error: " + e);
            e.printStackTrace();
            System.exit(1);
        }
    }

    public static String getVendor() {
        return VENDOR;
    }

    private static String initVendor() {
        String vmVendor = System.getProperty("java.vendor");
        return vmVendor == null ? "" : vmVendor.substring(0, vmVendor.indexOf(' '));
    }
}
