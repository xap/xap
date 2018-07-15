package org.gigaspaces.cli.commands;

import org.gigaspaces.cli.CliCommand;
import org.gigaspaces.cli.commands.utils.XapCliUtils;

import picocli.CommandLine.Command;

import java.util.ArrayList;
import java.util.List;

/**
 * @since 12.3
 * @author Rotem Herzberg
 */
@Command(name = "demo", header = "Run a Space in high availability mode (2 primaries with 1 backup each)")
public class DemoCommand extends CliCommand {

    private final String SPACE_NAME;
    private final boolean HA = true;
    private final int PARTITIONS_COUNT = 2;
    private final boolean isXapTest;

    public DemoCommand(){

        if ( System.getenv("IS_I9E") !=null && System.getenv("IS_I9E").equalsIgnoreCase("true")) {
            isXapTest=false;
            SPACE_NAME = XapCliUtils.DEMO_SPACE_NAME;
        }
        else{
            isXapTest=true;
            SPACE_NAME=XapCliUtils.INSIGHTEDGE_DEMO_SPACE_NAME;
        }

    }

    @Override
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
