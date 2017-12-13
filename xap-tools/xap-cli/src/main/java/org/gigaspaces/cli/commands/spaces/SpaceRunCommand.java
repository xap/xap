package org.gigaspaces.cli.commands.spaces;

import org.gigaspaces.cli.CliCommand;
import picocli.CommandLine.*;

import java.util.Arrays;

@Command(name="run", header = "Runs a standalone space")
public class SpaceRunCommand extends CliCommand {

    @Parameters(index = "0", description = "Name of space to run")
    String name;
    @Option(names = {"--partitions" }, description = "Number of partitions in space")
    int partitions;
    @Option(names = {"--ha" }, description = "Should the space include backups for high availability")
    boolean ha;
    @Option(names = {"--instances" }, split = ",", description = "Which instances should be run (default is all instances)")
    String[] instances;

    @Override
    protected void execute() throws Exception {
        String message = "Running space: " + name;
        if (partitions != 0)
            message += " with partitions=" + partitions + ", ha=" + ha;
        if (instances != null)
            message += ", instances=" + Arrays.toString(instances);
        System.out.println(message);
        underConstruction();
    }
}
