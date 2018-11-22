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

import com.gigaspaces.CommonSystemProperties;
import com.gigaspaces.start.SystemInfo;
import org.gigaspaces.cli.CliCommandException;
import org.gigaspaces.cli.JavaCommandBuilder;
import org.gigaspaces.cli.commands.utils.XapCliUtils;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.io.File;
import java.util.*;

/**
 * @since 12.3
 * @author Rotem Herzberg
 */
@Command(name="run", header = "Run a standalone Processing Unit")
public class PuRunCommand extends AbstractRunCommand {

    @Parameters(index = "0", description = "Relative/absolute path of a Processing Unit directory or archive file")
    File path;
    @Option(names = {"--partitions" }, description = "Specify the number of partitions for the Processing Unit")
    int partitions;
    @Option(names = {"--ha" }, description = "High availability (add one backup per partition)")
    boolean ha;
    @Option(names = {"--instances" }, split = ",", description = "Specify one or more instances to run (for example: --instances=1_1,1_2). "
                                                                    + "If no instances are specified, runs all instances.")
    List<String> instances;
    @Option(names = {"--lus"}, description = "Start a lookup service")
    boolean lus;

    @Override
    protected void execute() throws Exception {
        validateOptions(partitions, ha, instances);
        if (!path.exists())
            throw new CliCommandException("File not found: " + path);

        final List<ProcessBuilder> processBuilders = toProcessBuilders(instances, partitions, ha, lus);
        if (instances != null && !instances.isEmpty()) {
            throw new CliCommandException("Invalid instances: " + instances.toString());
        }
        XapCliUtils.executeProcesses(processBuilders);
    }

    @Override
    protected ProcessBuilder buildInstanceCommand(int id, boolean backup) {
        final JavaCommandBuilder command = new JavaCommandBuilder()
                .systemProperty(CommonSystemProperties.START_EMBEDDED_LOOKUP, "false")
                .optionsFromEnv("XAP_OPTIONS");
        command.classpathFromEnv("PRE_CLASSPATH");
        appendGsClasspath(command);
        command.classpathFromEnv("SPRING_JARS", () -> {
            //SPRING_JARS="%XAP_HOME%\lib\optional\spring\*:%XAP_HOME%\lib\optional\security\*"
            SystemInfo.XapLocations locations = SystemInfo.singleton().locations();
            command.classpath(locations.getLibOptional() + File.separator + "spring" + File.separator + "*");
            command.classpath(locations.getLibOptional() + File.separator + "security" + File.separator + "*");
        });
        command.classpathFromEnv("POST_CLASSPATH");
        command.mainClass("org.openspaces.pu.container.standalone.StandaloneProcessingUnitContainer");
        command.arg("-path").arg(path.getPath());
        if (id != 0) {
            command.arg("-cluster")
                    .arg("schema=partitioned")
                    .arg("total_members=" + partitions + "," + (ha ? "1" : "0"))
                    .arg("id=" + id)
                    .arg(backup ? "backup_id=1" : "");
        }

        return toProcessBuilder(command, "processing unit");
    }
}
