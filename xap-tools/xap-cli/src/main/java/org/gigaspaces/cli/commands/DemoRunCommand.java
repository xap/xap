package org.gigaspaces.cli.commands;

import org.gigaspaces.cli.commands.utils.XapCliUtils;

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
        XapCliUtils.executeProcesses(processBuilders);

    }
}
