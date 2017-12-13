package org.gigaspaces.cli.commands;

import org.gigaspaces.cli.CliCommand;
import org.gigaspaces.cli.SubCommandContainer;
import org.gigaspaces.cli.commands.pu.PuRunCommand;
import picocli.CommandLine.*;

import java.util.Arrays;
import java.util.Collection;

@Command(name="pu", headerHeading = XapMainCommand.HEADER, header = "interaction with Processing Units")
public class PUCommand extends CliCommand implements SubCommandContainer {

    @Override
    protected void execute() throws Exception {
    }

    @Override
    public Collection<Object> getSubCommands() {
        return Arrays.asList((Object) new PuRunCommand());
    }
}
