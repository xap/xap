package org.gigaspaces.cli.commands;

import org.gigaspaces.cli.CliCommand;
import org.gigaspaces.cli.CommandsSet;
import org.gigaspaces.cli.SubCommandContainer;
import picocli.CommandLine;

@CommandLine.Command(name="maven", header = "List of available commands for Maven-related operations")
public class MavenCommand extends CliCommand implements SubCommandContainer {
    @Override
    protected void execute() throws Exception {

    }

    @Override
    public CommandsSet getSubCommands() {
        CommandsSet result = new CommandsSet();
        result.add(new MavenInstallCommand());
        return result;
    }
}
