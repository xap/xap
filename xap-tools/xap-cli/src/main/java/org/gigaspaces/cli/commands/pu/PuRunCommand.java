package org.gigaspaces.cli.commands.pu;

import org.gigaspaces.cli.CliCommand;
import picocli.CommandLine.*;

import java.io.File;
import java.util.Arrays;

@Command(name="run", header = "Runs a standalone processing unit")
public class PuRunCommand extends CliCommand {

    @Parameters(index = "0", description = "The relative/absolute path of a processing unit directory or jar")
    File path;
    @Option(names = {"--partitions" }, description = "Number of partitions in processing unit")
    int partitions;
    @Option(names = {"--ha" }, description = "Should the processing unit include backups for high availability")
    boolean ha;
    @Option(names = {"--instances" }, split = ",", description = "Which instances should be run (default is all instances)")
    String[] instances;

    @Override
    protected void execute() throws Exception {
        String message = "Running processing Unit path: " + path;
        if (partitions != 0)
            message += " with partitions=" + partitions + ", ha=" + ha;
        if (instances != null)
            message += ", instances=" + Arrays.toString(instances);
        System.out.println(message);
        underConstruction();
    }
}
