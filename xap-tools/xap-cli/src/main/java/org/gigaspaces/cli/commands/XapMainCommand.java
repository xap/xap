package org.gigaspaces.cli.commands;

import org.gigaspaces.cli.CliCommand;
import org.gigaspaces.cli.CliCommandsSingleton;
import org.gigaspaces.cli.CliExecutor;
import org.gigaspaces.cli.SubCommandContainer;
import picocli.CommandLine.*;

import java.util.Arrays;
import java.util.Collection;

@Command(name="gs", headerHeading = XapMainCommand.HEADER, customSynopsis = "gs [global-options] command [options] [parameters]")
public class XapMainCommand extends CliCommand implements SubCommandContainer {
    public static final String HEADER =
                    "@|green   __   __          _____                                   |@%n" +
                    "@|green   \\ \\ / /    /\\   |  __ \\                               |@%n" +
                    "@|green    \\ V /    /  \\  | |__) |                                |@%n" +
                    "@|green     > <    / /\\ \\ |  ___/                                 |@%n" +
                    "@|green    / . \\  / ____ \\| |                                     |@%n" +
                    "@|green   /_/ \\_\\/_/    \\_\\_|                                   |@%n" +
                    "%n";

    @Option(names = {"--username" }, description = "Username for secured environments")
    protected String username;
    @Option(names = {"--password" }, description = "Password for secured environments")
    protected String password;

    protected void execute() throws Exception {
        CliCommandsSingleton.getInstance().setUsername( username );
        CliCommandsSingleton.getInstance().setPassword( password );
    }

    public static void main(String[] args) {
        CliExecutor.execute(new XapMainCommand(), args);
    }

    @Override
    public Collection<Object> getSubCommands() {
        return Arrays.asList(
                (Object)new HelpCommand(),
                new VersionCommand(),
                new PUCommand(),
                new SpaceCommand());
    }
}
