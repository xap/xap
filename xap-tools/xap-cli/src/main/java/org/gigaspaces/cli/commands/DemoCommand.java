package org.gigaspaces.cli.commands;

import org.gigaspaces.cli.CliCommand;
import org.gigaspaces.cli.ContinuousCommand;
import org.gigaspaces.cli.commands.utils.XapCliUtils;

import picocli.CommandLine.Command;

/**
 * @since 12.3
 * @author Rotem Herzberg
 */
@Command(name = "demo", header = "Run a Space in high availability mode (2 primaries with 1 backup each)")
public class DemoCommand extends CliCommand implements ContinuousCommand {

    @Override
    protected void execute() throws Exception {
        SpaceRunCommand command = new SpaceRunCommand();
        command.name = XapCliUtils.DEMO_SPACE_NAME;
        command.partitions = 2;
        command.ha = true;
        command.lus = true;
        command.execute();
    }
}
