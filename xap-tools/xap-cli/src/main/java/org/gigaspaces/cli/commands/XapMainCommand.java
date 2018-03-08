package org.gigaspaces.cli.commands;

import org.gigaspaces.cli.CliCommand;
import org.gigaspaces.cli.CliExecutor;
import org.gigaspaces.cli.SubCommandContainer;
import picocli.CommandLine.*;

import java.util.Arrays;
import java.util.Collection;

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

    protected void execute() throws Exception {
    }

    public static void main(String[] args) {
        CliExecutor.execute(new XapMainCommand(), args);
    }

    @Override
    public Collection<Object> getSubCommands() {
        return Arrays.asList(
                (Object)
                new VersionCommand(),
                new PUCommand(),
                new SpaceCommand(),
                new DemoCommand());
    }
}
