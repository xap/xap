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
public class DemoCommand {

    private final String SPACE_NAME = "demo-space";
    private final boolean HA = true;
    private final int PARTITIONS_COUNT = 2;

    //xap space run --partitions=2 --lus --ha demo-space
    // xap demo ^
    protected void execute() throws Exception {

        final List<ProcessBuilder> processBuilders = new ArrayList<ProcessBuilder>();
        processBuilders.add(AbstractRunCommand.buildStartLookupServiceCommand());
        processBuilders.add(SpaceRunCommand.buildPartitionedSpaceCommand(1, SPACE_NAME, HA, PARTITIONS_COUNT));
        processBuilders.add(SpaceRunCommand.buildPartitionedBackupSpaceCommand(1, SPACE_NAME, HA, PARTITIONS_COUNT));
        processBuilders.add(SpaceRunCommand.buildPartitionedSpaceCommand(2, SPACE_NAME, HA, PARTITIONS_COUNT));
        processBuilders.add(SpaceRunCommand.buildPartitionedBackupSpaceCommand(2, SPACE_NAME, HA, PARTITIONS_COUNT));
        XapCliUtils.executeProcesses(processBuilders);

    }
}
