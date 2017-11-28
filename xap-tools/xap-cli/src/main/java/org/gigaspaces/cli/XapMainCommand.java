package org.gigaspaces.cli;

import org.gigaspaces.cli.commands.VersionCommand;
import picocli.CommandLine;

import java.util.Arrays;
import java.util.Collection;

@CommandLine.Command(name="gs", header = {
        "@|green    _____           _       _     _   ______    _                 |@",
        "@|green   |_   _|         (_)     | |   | | |  ____|  | |                |@",
        "@|green     | |  _ __  ___ _  __ _| |__ | |_| |__   __| | __ _  ___      |@",
        "@|green     | | | '_ \\/ __| |/ _` | '_ \\| __|  __| / _` |/ _` |/ _ \\  |@",
        "@|green    _| |_| | | \\__ \\ | (_| | | | | |_| |___| (_| | (_| |  __/   |@",
        "@|green   |_____|_| |_|___/_|\\__, |_| |_|\\__|______\\__,_|\\__, |\\___||@",
        "@|green                       __/ |                       __/ |          |@",
        "@|green                      |___/                       |___/           |@",
})
public class XapMainCommand extends CliCommand {
    protected void execute() throws Exception {
    }

    public static void main(String[] args) {
        CliExecutor.main(args, new XapMainCommand(), getSubcommands());
    }

    public static Collection<?> getSubcommands() {
        return Arrays.asList(new VersionCommand());
    }
}
