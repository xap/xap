package org.gigaspaces.cli;

import com.gigaspaces.logger.Constants;

import picocli.CommandLine;
import picocli.CommandLine.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

import static java.lang.System.out;

public class CliExecutor {

    private static CommandLine mainCommandLine;

    public static CommandLine getMainCommand() {
        return mainCommandLine;
    }

    public static void execute(Object mainCommand, String[] args) {
        int exitCode;
        try {
            mainCommandLine = toCommandLine(mainCommand);
            mainCommandLine.parseWithHandler(new CustomResultHandler(), out, args);
            exitCode = 0;
        } catch (Exception e) {
            exitCode = handleException(e);
        }
        System.out.println();
        System.exit(exitCode);
    }

    public static void generateAutoComplete(Object mainCommand, String[] args) throws IOException {
        CommandLine commandLine = toCommandLine(mainCommand);
        String alias = args.length != 0 ? args[0] : commandLine.getCommandName();
        String generatedScript = picocli.AutoComplete.bash(alias, commandLine);
        try (FileWriter scriptWriter = new FileWriter(new File(alias + "-autocomplete"))) {
            scriptWriter.write(generatedScript);
        }
    }

    private static int handleException(Exception e) {
        if (e instanceof ExecutionException) {
            if (e.getCause() instanceof CliCommandException) {
                CliCommandException cause = (CliCommandException) e.getCause();
                printErr(cause);
                return cause.getExitCode();
            }
        }
        printErr(e);
        return 1;
    }

    private static void printErr(Throwable t) {
        String message = t.getLocalizedMessage();
        if (message == null) message = t.toString();
        System.err.println("\n[ERROR] " + message);

        if (!CliCommand.LOGGER.isLoggable(Level.FINE)) {
            System.err.println("- Configure " + Constants.LOGGER_CLI + " log level for verbosity");
        }
    }

    public static void printWarning(Throwable t) {
        String message = t.getLocalizedMessage();
        if (message == null) message = t.toString();
        System.err.println("\n[WARNING] " + message);
    }

    public static CommandLine toCommandLine(Object command) {
        CommandLine cmd = new CommandLine(command);
        if (command instanceof SubCommandContainer) {
            for (Map.Entry<String, Object> entry : ((SubCommandContainer) command).getSubCommands().getCommands().entrySet()) {
                Object subcommand = entry.getValue();
                if (subcommand instanceof SubCommandContainer)
                    subcommand = toCommandLine(subcommand);
                cmd.addSubcommand(entry.getKey(), subcommand);
            }
        }
        return cmd;
    }

    private static class CustomResultHandler extends CommandLine.RunAll {

        @Override
        public List<Object> handleParseResult(List<CommandLine> commands, PrintStream out, Help.Ansi ansi) throws ExecutionException {
            List<Object> result = super.handleParseResult(commands, out, ansi);
            if (!isHelpRequested(commands)) {
                CommandLine lastCommand = commands.get(commands.size()-1);
                if (lastCommand.getCommand() instanceof SubCommandContainer) {
                    //out.println("Command " + lastCommand.getCommandName() + " requires a sub-command");
                    lastCommand.usage(out, ansi);
                }
            }

            return result;
        }

        private boolean isHelpRequested(List<CommandLine> commands) {
            for (CommandLine command : commands)
                if (command.isUsageHelpRequested() || command.isVersionHelpRequested())
                    return true;

            return false;
        }
    }
}
