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
                throw new CliCommandException("Partitions must be defined when using high availability option");
            }
            if (instances != null) {
                throw new CliCommandException("Partitions must be defined when using instances option");
            }
        } else if (partitions < 0) {
            throw new CliCommandException("Partitions option must have a value above zero");
        }
    }

    public static ProcessBuilder buildStartLookupServiceCommand() {
        final ProcessBuilder pb = createJavaProcessBuilder();

        Collection<String> commands = new LinkedHashSet<String>();
        String[] options = {"XAP_LUS_OPTIONS", "XAP_OPTIONS"};
        addOptions(commands, options);

        commands.add("-classpath");
        commands.add(pb.environment().get("XAP_HOME") + File.pathSeparator + pb.environment().get("GS_JARS"));
        commands.add("com.gigaspaces.internal.lookup.LookupServiceFactory");

        pb.command().addAll(commands);
        showCommand("Starting Lookup Service with line:", pb.command());
        return pb;
    }

    public static ProcessBuilder createJavaProcessBuilder() {
        ProcessBuilder processBuilder = new ProcessBuilder(System.getenv("JAVACMD"));
        processBuilder.inheritIO();
        return processBuilder;
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
        LOGGER.fine(message + "\n" + commandline + "\n");
    }

    protected boolean containsInstance(String[] instances, String instance) {
        for (String s : instances) {
            if (s.equals(instance))
                return true;
        }
        return false;
    }
}
