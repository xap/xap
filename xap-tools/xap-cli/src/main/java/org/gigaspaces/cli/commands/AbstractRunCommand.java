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
import org.gigaspaces.cli.JavaCommandBuilder;

import java.util.logging.Level;

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

    protected static ProcessBuilder buildStartLookupServiceCommand() {
        final JavaCommandBuilder command = new JavaCommandBuilder()
                .optionsFromEnv("XAP_LUS_OPTIONS")
                .optionsFromEnv("XAP_OPTIONS")
                .mainClass("com.gigaspaces.internal.lookup.LookupServiceFactory");
        command.classpath(SystemInfo.singleton().getXapHome());
        appendGsClasspath(command);
        return toProcessBuilder(command, "lookup service");
    }

    protected static ProcessBuilder toProcessBuilder(JavaCommandBuilder command, String desc) {
        ProcessBuilder processBuilder = command.toProcessBuilder();
        processBuilder.inheritIO();
        if (LOGGER.isLoggable(Level.FINE)) {
            String message = "Starting " + desc + " with line:";
            String commandline = String.join(" ", processBuilder.command());
            LOGGER.fine(message + System.lineSeparator() + commandline + System.lineSeparator());
            //System.out.println(message + System.lineSeparator() + commandline + System.lineSeparator());
        }

        return processBuilder;
    }

    protected static void appendGsClasspath(JavaCommandBuilder command) {
        final SystemInfo.XapLocations locations = SystemInfo.singleton().locations();

        command.classpathFromEnv("GS_JARS", new Runnable() {
            @Override
            public void run() {
                //GS_JARS="%XAP_HOME%\lib\platform\ext\*";"%XAP_HOME%";"%XAP_HOME%\lib\required\*";"%XAP_HOME%\lib\optional\pu-common\*";"%XAP_CLASSPATH_EXT%"
                command.classpathFromPath(locations.getLibPlatform(), "ext", "*");
                command.classpathFromPath(locations.getLibRequired(),"*");
                command.classpathFromPath(locations.getLibOptional(), "pu-common", "*");
                command.classpathFromEnv("XAP_CLASSPATH_EXT");
            }
        });

        //fix for GS-13546
        command.classpathFromPath(locations.getLibPlatform(), "service-grid", "*");
        command.classpathFromPath(locations.getLibPlatform(), "logger", "*");
        command.classpathFromPath(locations.getLibPlatform(), "zookeeper", "*");
    }

    protected boolean containsInstance(String[] instances, String instance) {
        for (String s : instances) {
            if (s.equals(instance))
                return true;
        }
        return false;
    }
}
