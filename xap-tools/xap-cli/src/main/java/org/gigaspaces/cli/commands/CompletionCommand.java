package org.gigaspaces.cli.commands;

import org.gigaspaces.cli.CliCommand;
import org.gigaspaces.cli.CliExecutor;
import picocli.CommandLine;

import java.io.File;
import java.io.FileWriter;

@CommandLine.Command(name="completion", header = "Generate bash completion script")
public class CompletionCommand extends CliCommand {

    @CommandLine.Option(names = {"--target" }, description = "Target file for output (if not specified, will be printed to standard output)")
    private File target;

    @Override
    protected void execute() throws Exception {
        CommandLine mainCommand = CliExecutor.getMainCommand();
        String output = picocli.AutoComplete.bash(mainCommand.getCommandName(), mainCommand);
        if (target == null) {
            System.out.println(output);
        } else {
            try (FileWriter scriptWriter = new FileWriter(target)) {
                scriptWriter.write(output);
            }
        }
    }
}
