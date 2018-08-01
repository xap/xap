/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gigaspaces.cli.commands;

import com.gigaspaces.start.SystemInfo;
import org.gigaspaces.cli.CliCommand;
import org.gigaspaces.cli.CliCommandException;

import java.io.File;
import java.util.*;
import java.util.concurrent.*;

public abstract class AbstractRunCommand extends CliCommand {

    protected void validateOptions(int partitions, boolean ha, String[] instances) throws CliCommandException {
        //if partitions is not defined
        if (partitions == 0) {
            if (ha) {
                throw new CliCommandException("Missing argument: '--partitions' when used in conjunction with '--ha' option");
            }
            if (instances != null) {
                throw new CliCommandException("Missing argument: '--partitions' when used in conjunction with '--instances' option");
            }
        } else if (partitions < 0) {
            throw new CliCommandException("Illegal argument: '--partitions="+partitions+"' must be positive");
        }
    }

    public static ProcessBuilder buildStartLookupServiceCommand() {
        final ProcessBuilder pb = createJavaProcessBuilder();

        Collection<String> commands = new LinkedHashSet<String>();
        String[] options = {"XAP_LUS_OPTIONS", "XAP_OPTIONS"};
        addOptions(commands, options);

        commands.add("-classpath");
        commands.add(toClassPath(SystemInfo.singleton().getXapHome(), getGsJars(pb.environment())));
        commands.add("com.gigaspaces.internal.lookup.LookupServiceFactory");

        pb.command().addAll(commands);
        showCommand("Starting Lookup Service with line:", pb.command());
        return pb;
    }

    public static ProcessBuilder createJavaProcessBuilder() {
        ProcessBuilder processBuilder = new ProcessBuilder(getJavaCommand());
        processBuilder.inheritIO();
        return processBuilder;
    }

    private static String getJavaCommand() {
        String command = System.getenv("JAVACMD");
        if (command == null) {
            String javaHome = System.getenv("JAVA_HOME");
            if (javaHome == null)
                javaHome = System.getenv("XapNet.Runtime.JavaHome");
            if (javaHome != null)
                command = javaHome + File.separator + "bin" + File.separator + "java";
        }
        if (command == null)
            command = "java";
        return command;
    }

    protected static String toClassPath(String ... paths) {
        StringBuilder sb = new StringBuilder();
        for (String path : paths) {
            if (path == null || path.isEmpty())
                continue;
            if (sb.length() != 0)
                sb.append(File.pathSeparatorChar);
            sb.append(path);
        }

        return sb.toString();
    }

    protected static String getGsJars(Map<String, String> env) {
        String result = env.get("GS_JARS");
        if (result == null) {
            //set GS_JARS="%XAP_HOME%\lib\platform\ext\*";"%XAP_HOME%";"%XAP_HOME%\lib\required\*";"%XAP_HOME%\lib\optional\pu-common\*";"%XAP_CLASSPATH_EXT%"
            result = toClassPath(SystemInfo.singleton().locations().getLibPlatform() + File.separator + "ext" + File.separator + "*",
                    SystemInfo.singleton().locations().getLibRequired() + File.separator + "*",
                    SystemInfo.singleton().locations().getLibOptional() + File.separator + "pu-common" + File.separator + "*",
                    env.get("XAP_CLASSPATH_EXT"));

        }

        //fix for GS-13546
        String additionalClasspathLibs = toClassPath(
            SystemInfo.singleton().locations().getLibPlatform() + File.separator + "service-grid" + File.separator + "*",
                   SystemInfo.singleton().locations().getLibPlatform() + File.separator + "logger" + File.separator + "*",
                   SystemInfo.singleton().locations().getLibPlatform() + File.separator + "zookeeper" + File.separator + "*" );

        result = result + ( result.endsWith( File.pathSeparator ) ? "" : File.pathSeparator ) + additionalClasspathLibs;

        return result;
    }

    protected static String getSpringJars(Map<String, String> env) {
        String result = env.get("SPRING_JARS");
        if (result == null) {
            //set SPRING_JARS="%XAP_HOME%\lib\optional\spring\*;%XAP_HOME%\lib\optional\security\*;"
            result = toClassPath(SystemInfo.singleton().locations().getLibOptional() + File.separator + "spring" + File.separator + "*",
                    SystemInfo.singleton().locations().getLibOptional() + File.separator + "security" + File.separator + "*");
        }
        return result;
    }

    public static void addOptions(Collection<String> command, String[] options) {
        for (String option : options) {
            if (System.getenv(option) != null) {
                Collections.addAll(command, System.getenv(option).split(" "));
            }
        }
    }

    public static void showCommand(String message, List<String> command) {
        String commandline = command.toString().replace(",", "");
        if (commandline.length()>2) {
            commandline = commandline.substring(1, commandline.length() - 1);
        }
        LOGGER.fine(message + System.lineSeparator() + commandline + System.lineSeparator());
        //System.out.println(message + System.lineSeparator() + commandline + System.lineSeparator());
    }

    protected boolean containsInstance(String[] instances, String instance) {
        for (String s : instances) {
            if (s.equals(instance))
                return true;
        }
        return false;
    }
}
