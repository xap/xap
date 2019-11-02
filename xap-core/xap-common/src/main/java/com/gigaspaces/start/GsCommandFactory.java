package com.gigaspaces.start;

import com.gigaspaces.CommonSystemProperties;
import com.gigaspaces.internal.io.BootIOUtils;
import com.gigaspaces.internal.jvm.JavaUtils;
import com.gigaspaces.internal.utils.GsEnv;

import java.io.File;
import java.util.Collection;
import java.util.Collections;

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
        command.classpathFromPath(SystemInfo.singleton().locations().libPlatform("blueprints"));
        appendGsClasspath();
        // Options and system properties:
        appendXapOptions();
        appendServiceOptions(command, "CLI");

        return command;
    }

    public JavaCommandBuilder lus() {
        command.mainClass("com.gigaspaces.internal.lookup.LookupServiceFactory");
        appendXapOptions();
        command.optionsFromGsEnv("LUS_OPTIONS");

        command.classpath(SystemInfo.singleton().getXapHome());
        appendGsClasspath();
        //fix for GS-13546
        command.classpathFromPath(locations().libPlatform("service-grid"));
        command.classpathFromPath(locations().libPlatform("zookeeper"));

        return command;
    }

    public JavaCommandBuilder standalonePuInstance() {
        command.mainClass("org.openspaces.pu.container.standalone.StandaloneProcessingUnitContainer");
        appendXapOptions();
        preClasspath();
        appendGsClasspath();
        //fix for GS-13546
        command.classpathFromPath(locations().libPlatform("service-grid"));
        command.classpathFromPath(locations().libPlatform("zookeeper"));
        appendSpringClassPath();
        postClasspath();
        return command;
    }

    public JavaCommandBuilder spaceInstance() {
        command.mainClass("org.openspaces.pu.container.integrated.IntegratedProcessingUnitContainer");
        appendXapOptions();
        command.optionsFromGsEnv("SPACE_INSTANCE_OPTIONS");
        preClasspath();
        command.classpathFromPath(locations().deploy(), "templates", "datagrid");
        appendGsClasspath();
        //fix for GS-13546
        command.classpathFromPath(locations().libPlatform("service-grid"));
//        command.classpathFromPath(locations().libPlatform("oshi"));
        command.classpathFromPath(locations().libPlatform("zookeeper"));
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
        command.classpathFromPath(locations().libOptional("spring"));
        command.classpathFromPath(locations().libOptionalSecurity());
    }

    protected void appendSigarClassPath() {
        command.classpathFromPath(locations().libOptional("sigar"));
    }

    protected void appendOshiClassPath() {
        command.classpathFromPath(locations().libOptional("oshi"));
    }

    protected void appendMetricToolsClassPath() {
        appendSigarClassPath();
        appendOshiClassPath();
    }

    protected void appendXapOptions() {
        if (GsEnv.key("OPTIONS") != null) {
            command.optionsFromGsEnv("OPTIONS");
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

            command.systemProperty(CommonSystemProperties.GS_HOME, BootIOUtils.quoteIfContainsSpace(SystemInfo.singleton().getXapHome()));
            command.systemProperty("java.util.logging.config.file", BootIOUtils.quoteIfContainsSpace(GsEnv.getOrElse("LOGS_CONFIG_FILE", this::defaultConfigPath)));
            command.systemProperty("java.rmi.server.hostname", GsEnv.get("NIC_ADDRESS"));
            command.optionsFromEnv("EXT_JAVA_OPTIONS"); // Deprecated starting 14.5
            command.optionsFromGsEnv("OPTIONS_EXT");
        }
    }

    protected void appendGsClasspath() {
        // GS_JARS=$GS_HOME/lib/platform/ext/*:$GS_HOME:$GS_HOME/lib/required/*:$GS_HOME/lib/optional/pu-common/*:${GS_CLASSPATH_EXT}
        command.classpathFromPath(locations().libPlatformExt());
        command.classpathFromPath(SystemInfo.singleton().getXapHome());
        command.classpathFromPath(locations().libRequired());
        command.classpathFromPath(locations().libOptional("pu-common"));
        command.classpath(GsEnv.get("CLASSPATH_EXT"));
    }

    private String defaultConfigPath() {
        return locations().config("log").resolve("xap_logging.properties").toString();
    }

    protected void appendServiceOptions(JavaCommandBuilder command, String serviceType) {
        String envVarKey = GsEnv.key(serviceType.toUpperCase() + "_OPTIONS");
        if (envVarKey != null) {
            command.optionsFromEnv(envVarKey);
        } else {
            command.options(getDefaultOptions(serviceType));
        }
    }

    protected Collection<String> getDefaultOptions(String serviceType) {
        switch (serviceType) {
            case "CLI":
                return Collections.singletonList("-Xmx512m");
            default: return Collections.emptyList();
        }
    }

}
