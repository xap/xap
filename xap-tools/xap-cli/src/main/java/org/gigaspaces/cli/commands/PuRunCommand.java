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
    String[] instances;
    @Option(names = {"--lus"}, description = "Start a lookup service")
    boolean lus;

    @Override
    protected void execute() throws Exception {

        validateOptions(partitions, ha, instances);

        final List<ProcessBuilder> processBuilders = new ArrayList<ProcessBuilder>();
        if (lus) {
            processBuilders.add(buildStartLookupServiceCommand());
        }
        if (partitions == 0) {
            processBuilders.add(buildSinglePuCommand());
        } else {
            for (int id = 1; id < partitions+1; id++) {
                if (instances == null) {
                    processBuilders.add(buildPartitionedPuCommand(id));
                    if (ha) {
                        processBuilders.add(buildPartitionedBackupPuCommand(id));
                    }
                } else {
                    if (containsInstance(instances, id + "_" + 1)) {
                        processBuilders.add(buildPartitionedPuCommand(id));
                    }
                    if (containsInstance(instances, id + "_" + 2)) {
                        processBuilders.add(buildPartitionedBackupPuCommand(id));
                    }
                }
            }
        }
        XapCliUtils.executeProcesses(processBuilders);
    }

    private ProcessBuilder buildSinglePuCommand(){

        final ProcessBuilder pb = createJavaProcessBuilder();
        final Collection<String> commands = new LinkedHashSet<String>();
        commands.add("-Dcom.gs.start-embedded-lus=false");

        String[] options = {"XAP_OPTIONS"};
        addOptions(commands, options);

        commands.add("-classpath");
        commands.add(getProcessingUnitClassPath(pb.environment()));
        commands.add("org.openspaces.pu.container.standalone.StandaloneProcessingUnitContainer");
        commands.add("-path");
        commands.add(path.getPath());

        pb.command().addAll(commands);
        showCommand("Starting Process Unit with line:", pb.command());
        return pb;
    }

    private ProcessBuilder buildPartitionedPuCommand(int id) {
        return buildPartitionedPuCommand(id, false);
    }

    private ProcessBuilder buildPartitionedBackupPuCommand(int id) {
        return buildPartitionedPuCommand(id, true);
    }

    private static String getProcessingUnitClassPath(Map<String, String> env) {
        return toClassPath(env.get("PRE_CLASSPATH"), getGsJars(env), getSpringJars(env), env.get("POST_CLASSPATH"));
    }

    private ProcessBuilder buildPartitionedPuCommand(int id, boolean backup) {

        final ProcessBuilder pb = createJavaProcessBuilder();
        final Collection<String> commands = new LinkedHashSet<String>();
        commands.add("-Dcom.gs.start-embedded-lus=false");

        String[] options = {"XAP_OPTIONS"};
        addOptions(commands, options);

        commands.add("-classpath");
        commands.add(getProcessingUnitClassPath(pb.environment()));
        commands.add("org.openspaces.pu.container.standalone.StandaloneProcessingUnitContainer");
        commands.add("-path");
        commands.add(path.getPath());

        commands.add("-cluster");
        commands.add("schema=partitioned");
        if(ha){
            commands.add("total_members=" + partitions + ",1");
        } else{
            commands.add("total_members=" + partitions + ",0");
        }
        commands.add("id=" + id);
        if(backup){
            commands.add("backup_id=1");
        }

        pb.command().addAll(commands);
        showCommand("Starting Process Unit with line:", pb.command());
        return pb;
    }
}
