package org.gigaspaces.cli;

import picocli.CommandLine;
import picocli.CommandLine.Command;

import java.util.Collection;

public class CliExecutor {

    private static CliExecutor instance;
    private final CommandLine cmd;

    public static CliExecutor getInstance() {
        return instance;
    }

    public static void main(String[] args, Object mainCommand, Collection<?> subcommands) {
        instance = new CliExecutor(mainCommand, subcommands);
        int exitCode = instance.execute(args);
        System.exit(exitCode);
    }

    private CliExecutor(Object mainCommand, Collection<?> subcommands) {
        this.cmd = new CommandLine(mainCommand);
        for (Object subcommand : subcommands) {
            Command commandAnnotation = (Command) subcommand.getClass().getAnnotation(Command.class);
            cmd.addSubcommand(commandAnnotation.name(), subcommand);
        }
    }

    private int execute(String[] args) {
        int exitCode;
        try {
            // TODO: This too naive - what if args is no empty but no commands to run?
            if (args.length == 0)
                cmd.usage(System.out);
            else
                cmd.parseWithHandler(new CommandLine.RunAll(), System.out, args);
            exitCode = 0;
        } catch (Exception e) {
            if (e instanceof CommandLine.ExecutionException && e.getCause() instanceof CliCommandException) {
                CliCommandException cause = (CliCommandException) e.getCause();
                System.out.println(cause.getMessage());
                exitCode = cause.getExitCode();
            } else {
                e.printStackTrace();
                exitCode = 1;
            }
        }
        return exitCode;
    }

    public CommandLine getCmd() {
        return cmd;
    }
}
