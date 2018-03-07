package org.gigaspaces.cli.commands;

import org.gigaspaces.cli.CliCommand;
import org.gigaspaces.cli.SubCommandContainer;
import picocli.CommandLine.*;

import java.util.Arrays;
import java.util.Collection;

@Command(name="pu", headerHeading = XapMainCommand.HEADER, header = "List of available commands for Processing Unit operations")
public class PUCommand extends CliCommand implements SubCommandContainer {

    @Override
    protected void execute() throws Exception {
    }

    @Override
    public Collection<Object> getSubCommands() {
        return Arrays.asList((Object) new PuRunCommand());
    }
}
