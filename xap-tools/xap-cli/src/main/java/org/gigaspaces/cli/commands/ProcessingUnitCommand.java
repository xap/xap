package org.gigaspaces.cli.commands;

import org.gigaspaces.cli.CliCommand;
import org.gigaspaces.cli.CommandsSet;
import org.gigaspaces.cli.SubCommandContainer;
import picocli.CommandLine.*;

@Command(name="service", aliases = {"pu"}, header = "List of available commands for Processing Unit operations")
public class ProcessingUnitCommand extends CliCommand implements SubCommandContainer {

    @Override
    protected void execute() throws Exception {
    }

    @Override
    public CommandsSet getSubCommands() {
        CommandsSet result = new CommandsSet();
        // This command is not supported in XAP.NET
        if (!XapMainCommand.isXapNet())
            result.add(new PuRunCommand());
        return result;
    }
}
