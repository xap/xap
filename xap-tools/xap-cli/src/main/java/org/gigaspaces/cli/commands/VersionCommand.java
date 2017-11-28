package org.gigaspaces.cli.commands;

import com.gigaspaces.internal.version.PlatformVersion;
import org.gigaspaces.cli.CliCommand;
import picocli.CommandLine.*;

@Command(name="version", header = "Prints version information")
public class VersionCommand extends CliCommand {
    @Override
    protected void execute() throws Exception {
        System.out.println(PlatformVersion.getOfficialVersion());
    }
}
