package org.gigaspaces.cli.commands;

import org.gigaspaces.cli.CliCommand;
import org.gigaspaces.cli.CliExecutor;
import org.gigaspaces.cli.CommandsSet;
import org.gigaspaces.cli.SubCommandContainer;
import picocli.CommandLine.*;

@Command(name="xap", headerHeading = XapMainCommand.HEADER, customSynopsis = "xap [global-options] command [options] [parameters]")
public class XapMainCommand extends CliCommand implements SubCommandContainer {
    public static final String HEADER =
                    "@|green   __   __          _____                                   |@%n" +
                    "@|green   \\ \\ / /    /\\   |  __ \\                               |@%n" +
                    "@|green    \\ V /    /  \\  | |__) |                                |@%n" +
                    "@|green     > <    / /\\ \\ |  ___/                                 |@%n" +
                    "@|green    / . \\  / ____ \\| |                                     |@%n" +
                    "@|green   /_/ \\_\\/_/    \\_\\_|                                   |@%n" +
                    "%n";

    private static boolean isXapNet = System.getProperty("com.gs.xapnet.home") != null;

    protected void execute() throws Exception {
    }

    public static void main(String[] args) {
        CliExecutor.execute(new XapMainCommand(), args);
    }

    @Override
    public CommandsSet getSubCommands() {
        CommandsSet commandsSet = new CommandsSet();
        commandsSet.add(new VersionCommand());
        commandsSet.add(new HelpCommand());
        commandsSet.add(new DemoCommand());
        // This command is not supported in XAP.NET
        if (!isXapNet())
            commandsSet.add(new BlueprintCommand());
        commandsSet.add(new ProcessingUnitCommand());
        commandsSet.add(new SpaceCommand());
        // This command is not supported in XAP.NET
        if (!isXapNet())
            commandsSet.add(new MavenCommand());
        return commandsSet;
    }

    public static boolean isXapNet() {
        return isXapNet;
    }
}
