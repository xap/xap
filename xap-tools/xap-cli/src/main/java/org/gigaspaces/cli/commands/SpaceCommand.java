package org.gigaspaces.cli.commands;

import com.gigaspaces.start.SystemInfo;
import org.gigaspaces.cli.CliCommand;
import org.gigaspaces.cli.SubCommandContainer;
import picocli.CommandLine.*;

import java.util.ArrayList;
import java.util.Collection;

@Command(name="space", header = "List of available commands for Space operations")
public class SpaceCommand extends CliCommand implements SubCommandContainer {

    @Override
    protected void execute() throws Exception {
    }

    @Override
    public Collection<Object> getSubCommands() {
        Collection<Object> result = new ArrayList<Object>();
        // This command is not supported in XAP.NET
        if (SystemInfo.singleton().locations().xapNetHome() == null)
            result.add(new SpaceRunCommand());
        return result;
    }
}
