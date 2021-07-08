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
import com.gigaspaces.start.GsCommandFactory;
import org.gigaspaces.cli.CliCommandException;
import com.gigaspaces.start.JavaCommandBuilder;
import org.gigaspaces.cli.ContinuousCommand;
import org.gigaspaces.cli.commands.utils.XapCliUtils;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.util.*;

/**
 * @since 12.3
 * @author Rotem Herzberg
 */
@Command(name = "run", header = "Run a standalone Space")
public class SpaceRunCommand extends AbstractRunCommand implements ContinuousCommand {

    @Parameters(index = "0", description = "Name of Space to run")
    public String name;
    @Option(names = {"--partitions"}, description = "Specify the number of partitions for the Processing Unit")
    public int partitions;
    @Option(names = {"--ha"}, description = "High availability (add one backup per partition)")
    public boolean ha;
    @Option(names = {"--instances"}, split = ",", description = "Specify one or more instances to run (for example: --instances=1_1,1_2). "
                                                                    + "If no instances are specified, runs all instances.")
    List<String> instances;
    @Option(names = {"--lus"}, description = "Start a lookup service")
    public boolean lus;

    @Override
    public void validate() throws CliCommandException {
        validateOptions(partitions, ha, instances);
    }

    @Override
    protected void execute() throws Exception {
        validate();
        XapCliUtils.executeProcesses(toProcessBuilders());
    }

    public List<ProcessBuilder> toProcessBuilders() {
        return toProcessBuilders(instances, partitions, ha, lus);
    }

    @Override
    protected ProcessBuilder buildInstanceCommand(int id, boolean isBackup) {
        JavaCommandBuilder command = new CommandBuilder(name)
                .topology(partitions, ha)
                .instance(id, isBackup)
                .toCommand();
        return toProcessBuilder(command, "space");
    }

    public static class CommandBuilder {
        final String name;
        private int partitions;
        private boolean ha;
        private int partitionId;
        private boolean isBackupInstance;
        private String javaHeap;

        public CommandBuilder(String name) {
            this.name = name;
        }

        public CommandBuilder topology(int partitions, boolean ha) {
            this.partitions = partitions;
            this.ha = ha;
            return this;
        }

        public CommandBuilder instance(int partitionId, boolean isBackupInstance) {
            this.partitionId = partitionId;
            this.isBackupInstance = isBackupInstance;
            return this;
        }

        public JavaCommandBuilder toCommand() {
            final JavaCommandBuilder command = new GsCommandFactory().spaceInstance()
                    .systemProperty(CommonSystemProperties.START_EMBEDDED_LOOKUP, "false")
                    .heap(javaHeap);

            command.arg("-name").arg(name);
            if (partitionId != 0) {
                command.arg("-cluster")
                        .arg("schema=partitioned")
                        .arg("total_members=" + partitions + "," + (ha ? "1" : "0"))
                        .arg("id=" + partitionId)
                        .arg(isBackupInstance ? "backup_id=1" : "");
            }

            return command;
        }

        public CommandBuilder javaHeap(String javaHeap) {
            this.javaHeap = javaHeap;
            return this;
        }
    }
}
