package com.gigaspaces.internal.jvm;

import com.gigaspaces.api.InternalApi;
import com.gigaspaces.internal.utils.LazySingleton;
import com.sun.management.HotSpotDiagnosticMXBean;
import com.sun.management.VMOption;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.management.ManagementFactory;

@InternalApi
public class JavaUtils {
    private static final String VENDOR = System.getProperty("java.vendor", "");
    private static final String VERSION = System.getProperty("java.version", "");
    private static final int JAVA_VERSION_MAJOR = parseJavaMajorVersion(VERSION);
    private static final boolean isWindows = System.getProperty("os.name").toLowerCase().startsWith("win");
    private static final boolean isOsx = System.getProperty("os.name").toLowerCase().startsWith("mac os x");
    private static final LazySingleton<HotSpotDiagnosticMXBean> hotSpotDiagnosticMXBean = new LazySingleton<>(JavaUtils::initHotspotMBean);
    private static final LazySingleton<Long> pid = new LazySingleton<>(JavaUtils::findProcessId);

    /**
     * Starting Java 9, format is: MAJOR.MINOR.SECURITY, where trailing 0 are omitted.
     * For example: 9, 9.0.1, etc.
     * Older versions format is 1.MAJOR.suffix
     * For example: 1.8.144, 1.7, etc.
     */
    private static int parseJavaMajorVersion(String version) {
        // Remove "1." if exists (versions before 9):
        if (version.startsWith("1."))
            version = version.substring(2);
        // Find and remove everything after major, if any:
        int pos = version.indexOf('.');
        if (pos != -1)
            version = version.substring(0, pos);
        return Integer.parseInt(version);
    }

    public static String getVersion() {
        return VERSION;
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

    /**
     * Dumps the heap, note generally you only want live objects (does garage collection first)
     * @param filename - name of the file
     * @param live - only live or include uncollected objects
     * @throws IOException - thrown if there is an error
     */
    public static void dumpHeap(String filename, boolean live) throws IOException {
        hotSpotDiagnosticMXBean.getOrCreate().dumpHeap(filename, live);
    }

    public static VMOption getVMOption(String name) {
        return hotSpotDiagnosticMXBean.getOrCreate().getVMOption(name);
    }

    public static boolean useCompressedOops() {
        VMOption vmOption = getVMOption("UseCompressedOops");
        String val = vmOption != null ? vmOption.getValue() : null;
        return Boolean.parseBoolean(val);
    }

    private static HotSpotDiagnosticMXBean initHotspotMBean()  {
        try {
            return ManagementFactory.newPlatformMXBeanProxy(ManagementFactory.getPlatformMBeanServer(),
                    "com.sun.management:type=HotSpotDiagnostic", HotSpotDiagnosticMXBean.class);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
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

    public static long getPid() {
        return pid.getOrCreate();
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
}
