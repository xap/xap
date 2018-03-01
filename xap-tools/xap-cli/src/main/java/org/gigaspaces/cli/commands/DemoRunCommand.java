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

import picocli.CommandLine.Command;

import java.util.ArrayList;
import java.util.List;

/**
 * @since 12.3
 * @author Rotem Herzberg
 */
@Command(name = "demo", headerHeading = XapMainCommand.HEADER, header = "Runs a demo Partitioned Space with two partitions and backups")
public class DemoRunCommand extends AbstractRunCommand {

    //xap space run --partitions=2 --lus --ha demo-space
    // xap demo ^
    @Override
    protected void execute() throws Exception {

        final List<ProcessBuilder> processBuilders = new ArrayList<ProcessBuilder>();
        processBuilders.add(buildStartLookupServiceCommand());
        processBuilders.add(SpaceRunCommand.buildPartitionedSpaceCommand(1, "demo-space", true, 2));
        processBuilders.add(SpaceRunCommand.buildPartitionedBackupSpaceCommand(1, "demo-space", true, 2));
        processBuilders.add(SpaceRunCommand.buildPartitionedSpaceCommand(2, "demo-space", true, 2));
        processBuilders.add(SpaceRunCommand.buildPartitionedBackupSpaceCommand(2, "demo-space", true, 2));
        executeProcesses(processBuilders);

    }
}
