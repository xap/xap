package com.gigaspaces.start;

import com.gigaspaces.internal.io.BootIOUtils;
import com.gigaspaces.internal.jvm.JavaUtils;

import java.io.File;
import java.util.Optional;

/**
 * @author Niv Ingberg
 * @since 14.2
 */
public class GsCommandFactory {
    protected final JavaCommandBuilder command = new JavaCommandBuilder();
    protected String[] args;

    public static void main(String[] args) {
        execute(args, new GsCommandFactory());
    }

    protected static void execute(String[] args, GsCommandFactory builder) {
        try {
            builder.args = args;
            String s = builder.generate(args[0]).toCommandLine();
            System.out.println(s);
            System.exit(0);
        } catch (Throwable e) {
            System.out.println("Error: " + e);
            System.exit(1);
        }
    }

    protected JavaCommandBuilder generate(String id) {
        switch (id.toLowerCase()) {
            case "cli": return cli();
            default: return custom(id);
        }
    }

    private JavaCommandBuilder custom(String mainClass) {
        command.mainClass(mainClass);
        appendGsClasspath();
        appendSpringClassPath();
        appendXapOptions();
        return command;
    }

    protected JavaCommandBuilder cli() {
        command.mainClass("org.gigaspaces.cli.commands.XapMainCommand");
        // Class path:
        command.classpathFromPath(SystemInfo.singleton().getXapHome(), "tools", "cli", "*");
        command.classpathFromPath(SystemInfo.singleton().locations().getLibPlatform(), "blueprints", "*");
        appendGsClasspath();
        // Options and system properties:
        appendXapOptions();
        command.optionsFromEnv("XAP_CLI_OPTIONS");

        return command;
    }

    public JavaCommandBuilder lus() {
        command.mainClass("com.gigaspaces.internal.lookup.LookupServiceFactory");
        appendXapOptions();
        command.optionsFromEnv("XAP_LUS_OPTIONS");

        command.classpath(SystemInfo.singleton().getXapHome());
        appendGsClasspath();
        //fix for GS-13546
        command.classpathFromPath(locations().getLibPlatform(), "service-grid", "*");
        command.classpathFromPath(locations().getLibPlatform(), "zookeeper", "*");

        return command;
    }

    public JavaCommandBuilder standalonePuInstance() {
        command.mainClass("org.openspaces.pu.container.standalone.StandaloneProcessingUnitContainer");
        appendXapOptions();
        preClasspath();
        appendGsClasspath();
        //fix for GS-13546
        command.classpathFromPath(locations().getLibPlatform(), "service-grid", "*");
        command.classpathFromPath(locations().getLibPlatform(), "zookeeper", "*");
        appendSpringClassPath();
        postClasspath();
        return command;
    }

    public JavaCommandBuilder spaceInstance() {
        command.mainClass("org.openspaces.pu.container.integrated.IntegratedProcessingUnitContainer");
        appendXapOptions();
        command.optionsFromEnv("XAP_SPACE_INSTANCE_OPTIONS");
        preClasspath();
        command.classpathFromPath(locations().deploy(), "templates", "datagrid");
        appendGsClasspath();
        //fix for GS-13546
        command.classpathFromPath(locations().getLibPlatform(), "service-grid", "*");
//        command.classpathFromPath(locations().getLibPlatform(), "oshi", "*");
        command.classpathFromPath(locations().getLibPlatform(), "zookeeper", "*");
        postClasspath();

        return command;
    }

    protected SystemInfo.XapLocations locations() {
        return SystemInfo.singleton().locations();
    }

    protected void preClasspath() {
        command.classpathFromEnv("PRE_CLASSPATH");
    }

    protected void postClasspath() {
        command.classpathFromEnv("POST_CLASSPATH");
    }

    protected void appendSpringClassPath() {
        command.classpathFromPath(locations().getLibOptional(), "spring", "*");
        command.classpathFromPath(locations().getLibOptional(), "security", "*");
    }

    protected void appendSigarClassPath() {
        command.classpathFromPath(locations().getLibOptional(), "sigar", "*");
    }

    protected void appendOshiClassPath() {
        command.classpathFromPath(locations().getLibOptional(), "oshi", "*");
    }

    protected void appendMetricToolsClassPath() {
        appendSigarClassPath();
        appendOshiClassPath();
    }

    protected void appendXapOptions() {
        if (getSystemEnv("XAP_OPTIONS").isPresent()) {
            command.optionsFromEnv("XAP_OPTIONS");
        } else {
            final String vendor = JavaUtils.getVendor().toUpperCase();
            if (vendor.startsWith("ORACLE ")) {
                command.option("-server");
                if (!JavaUtils.greaterOrEquals(11)) {
                    // Deprecated since 11 - https://bugs.openjdk.java.net/browse/JDK-8199777
                    command.option("-XX:+AggressiveOpts");
                }
                command.option("-XX:+HeapDumpOnOutOfMemoryError");
            } else if (vendor.startsWith("IBM ")) {
                command.option("-XX:MaxPermSize=256m");
            }
            if (JavaUtils.greaterOrEquals(9)) {
                command.option("--add-opens=jdk.management/com.sun.management.internal=ALL-UNNAMED");
                command.option("--add-modules=ALL-SYSTEM");
            }

            command.systemProperty("com.gs.home", BootIOUtils.quoteIfContainsSpace(SystemInfo.singleton().getXapHome()));
            command.systemProperty("java.util.logging.config.file", BootIOUtils.quoteIfContainsSpace(getSystemEnv("XAP_LOGS_CONFIG_FILE").orElseGet(this::defaultConfigPath)));
            command.systemProperty("java.rmi.server.hostname", System.getenv("XAP_NIC_ADDRESS"));
            command.optionsFromEnv("EXT_JAVA_OPTIONS"); // Deprecated starting 14.5
            command.optionsFromEnv("XAP_OPTIONS_EXT");
        }
    }

    protected void appendGsClasspath() {
        // GS_JARS=$XAP_HOME/lib/platform/ext/*:$XAP_HOME:$XAP_HOME/lib/required/*:$XAP_HOME/lib/optional/pu-common/*:${XAP_CLASSPATH_EXT}
        command.classpathFromPath(locations().getLibPlatform(), "ext", "*");
        command.classpathFromPath(SystemInfo.singleton().getXapHome());
        command.classpathFromPath(locations().getLibRequired(),"*");
        command.classpathFromPath(locations().getLibOptional(), "pu-common", "*");
        command.classpathFromEnv("XAP_CLASSPATH_EXT");
    }

    private String defaultConfigPath() {
        return locations().config() + File.separator + "log" + File.separator + "xap_logging.properties";
    }

    protected static Optional<String> getSystemEnv(String name) {
        String val = System.getenv(name);
        return val != null ? Optional.of(val) : Optional.empty();
    }
}
