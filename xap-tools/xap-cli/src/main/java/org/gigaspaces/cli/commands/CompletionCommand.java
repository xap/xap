package org.gigaspaces.cli.commands;

import org.gigaspaces.cli.CliCommand;
import org.gigaspaces.cli.CliExecutor;
import picocli.CommandLine;

import java.io.File;
import java.io.FileWriter;

@CommandLine.Command(name="completion", header = {
        "Generate completion script for bash/zsh shells",
        "The generated script must be evaluated to provide interactive completion of gigaspaces commands.  This can be done by sourcing it from the .bash _profile.",
        "",
        "Detailed instructions on how to do this are available here:",
        "https://docs.gigaspaces.com/14.5/admin/tools-cli.html#AutocompleteFunctionality",
        "",
        "Examples:",
        "  # Installing bash completion on Linux",
        "  ## Load the gs completion code for bash into the current shell",
        "  source <(./gs completion)",
        "",
        "  # Installing bash completion on macOS using homebrew",
        "  ./gs completion > $(brew --prefix)/etc/bash_completion.d/gs"
        })
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
