package org.gigaspaces.cli.commands;

import org.gigaspaces.cli.CliCommand;
import org.gigaspaces.cli.CliExecutor;
import picocli.CommandLine.*;

@Command(name="help", header = "Help information for this command")
public class HelpCommand extends CliCommand {
    @Override
    protected void execute() throws Exception {
        CliExecutor.getMainCommand().usage(System.out);
    }
}
