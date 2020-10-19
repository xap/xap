package com.gigaspaces.start;

import com.gigaspaces.internal.io.BootIOUtils;
import com.gigaspaces.internal.utils.GsEnv;

import java.io.File;
import java.nio.file.Path;
import java.util.*;

/**
 * @author Niv Ingberg
 * @since 14.0
 */
public class JavaCommandBuilder {

    private final Map<String, String> env = System.getenv();
    private String javaExecutable;
    private final Map<String, String> systemProperties = new LinkedHashMap<>();
    private final Collection<String> options = new LinkedHashSet<>();
    private String mainClass;
    private final Collection<String> arguments = new ArrayList<>();
    private final Collection<String> classpath = new LinkedHashSet<>();

    public List<String> build() {
        List<String> command = new ArrayList<>();
        command.add(!isEmpty(javaExecutable) ? javaExecutable : getDefaultJavaExecutable());
        systemProperties.forEach((k, v) -> command.add("-D" + k + "=" + v));
        command.addAll(options);
        command.add("-classpath");
        command.add(String.join(File.pathSeparator, classpath));
        command.add(mainClass);
        command.addAll(arguments);
        return command;
    }

    public ProcessBuilder toProcessBuilder() {
        return new ProcessBuilder(build());
    }

    public String toCommandLine() {
        return String.join(" ", build());
    }

    public JavaCommandBuilder javaExecutable(String javaExecutable) {
        this.javaExecutable = javaExecutable;
        return this;
    }

    public JavaCommandBuilder mainClass(String mainClass) {
        this.mainClass = mainClass;
        return this;
    }

    public JavaCommandBuilder classpath(String classpath) {
        if (!isEmpty(classpath)) {
            this.classpath.add(BootIOUtils.quoteIfContainsSpace(classpath));
        }
        return this;
    }

    public JavaCommandBuilder classpath(Path path) {
        return classpath(path.toString());
    }

    public JavaCommandBuilder classpath(Collection<Path> paths)  {
        for (Path path : paths)
            classpath(path);
        return this;
    }

    public JavaCommandBuilder classpathWithJars(Path path) {
        return classpath(path.toString() + File.separator + "*");
    }

    public JavaCommandBuilder classpathFromEnv(String envVarName) {
        return classpath(env.get(envVarName));
    }

    public JavaCommandBuilder systemProperty(String key, String value) {
        systemProperties.put(key, value);
        return this;
    }

    public JavaCommandBuilder option(String option) {
        if (!isEmpty(option))
            options.add(option);
        return this;
    }

    public JavaCommandBuilder options(Collection<String> options) {
        this.options.addAll(options);
        return this;
    }

    public JavaCommandBuilder optionsFromEnv(String envVarName) {
        String s = env.get(envVarName);
        if (!isEmpty(s)) {
            for (String option : s.split(" (?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")) {
                option(option);
            }
        }
        return this;
    }

    public JavaCommandBuilder optionsFromGsEnv(String envVarSuffix) {
        String s = GsEnv.get(envVarSuffix);
        if (!isEmpty(s)) {
            for (String option : s.split(" (?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")) {
                option(option);
            }
        }
        return this;
    }

    public JavaCommandBuilder systemPropertyFromEnv(String key, String envVarName) {
        String s = env.get(envVarName);
        if (!isEmpty(s)) {
            systemProperty(key, s);
        }
        return this;
    }

    public String getEnvVarValue(String envVarName){
        return env.get(envVarName);
    }

    public JavaCommandBuilder heap(String heap) {
        initialHeap(heap);
        maxHeap(heap);
        return this;
    }

    public JavaCommandBuilder initialHeap(String heap) {
        if (!isEmpty(heap))
            options.add("-Xms" + heap);
        return this;
    }

    public JavaCommandBuilder maxHeap(String heap) {
        if (!isEmpty(heap))
            options.add("-Xmx" + heap);
        return this;
    }

    public JavaCommandBuilder arg(String arg) {
        if (!isEmpty(arg))
            arguments.add(arg);
        return this;
    }

    private String getDefaultJavaExecutable() {
        String command = env.get("JAVACMD");
        if (command == null) {
            String javaHome = env.get("JAVA_HOME");
            if (javaHome == null)
                javaHome = env.get("XapNet.Runtime.JavaHome");
            if (javaHome != null)
                command = javaHome + File.separator + "bin" + File.separator + "java";
        }
        if (command == null)
            command = "java";
        return command;
    }

    public String getDefaultJavaWindowsExecutable() {
        String command = env.get("JAVAWCMD");
        if (command == null) {
            String javaHome = env.get("JAVA_HOME");
            if (javaHome == null)
                javaHome = env.get("XapNet.Runtime.JavaHome");
            if (javaHome != null)
                command = javaHome + File.separator + "bin" + File.separator + "javaw";
        }
        if (command == null)
            command = "javaw";
        return command;
    }

    private static boolean isEmpty(String s) {
        return s == null || s.length() == 0;
    }
}
