package org.gigaspaces.cli.commands;

import org.gigaspaces.cli.CliCommand;
import picocli.CommandLine.*;

@Command(name="version", header = "Prints version information")
public class VersionCommand extends CliCommand {
    @Override
    protected void execute() throws Exception {
        for (String s : new XapVersionProvider().getVersion()) {
            System.out.println(s);
        }
    }
}
