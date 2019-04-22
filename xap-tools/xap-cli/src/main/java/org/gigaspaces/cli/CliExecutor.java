package org.gigaspaces.cli;

import com.gigaspaces.logger.Constants;

import org.jline.reader.*;
import org.jline.reader.impl.DefaultParser;
import org.jline.reader.impl.LineReaderImpl;
import org.jline.terminal.TerminalBuilder;
import picocli.AutoComplete;
import picocli.CommandLine;
import picocli.CommandLine.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

public class CliExecutor {

    private static CommandLine mainCommandLine;

    public static CommandLine getMainCommand() {
        return mainCommandLine;
    }

    public static void execute(Object mainCommand, String[] args) {
        int exitCode;
        try {
            mainCommandLine = toCommandLine(mainCommand);
            if (args.length == 0) {
                System.out.println("Starting interactive shell...");
                executeShell();
            } else {
                execute(args);
            }
            exitCode = 0;
        } catch (Exception e) {
            exitCode = handleException(e);
        }
        System.out.println();
        System.exit(exitCode);
    }

    private static void execute(String[] args) {
        mainCommandLine.parseWithHandler(new CustomResultHandler(), System.out, args);
    }

    public static void generateAutoComplete(Object mainCommand, String[] args) throws IOException {
        CommandLine commandLine = toCommandLine(mainCommand);
        String alias = args.length != 0 ? args[0] : commandLine.getCommandName();
        String generatedScript = picocli.AutoComplete.bash(alias, commandLine);
        try (FileWriter scriptWriter = new FileWriter(new File(alias + "-autocomplete"))) {
            scriptWriter.write(generatedScript);
        }
    }

    private static void executeShell() {
        mainCommandLine.addSubcommand("exit", ShellExitCommand.instance);
        mainCommandLine.addSubcommand("cls", ShellClearCommand.instance);

        try {
            // set up the completion
            LineReader reader = LineReaderBuilder.builder()
                    .terminal(TerminalBuilder.builder().build())
                    .completer(new PicocliJLineCompleter(mainCommandLine.getCommandSpec()))
                    .parser(new DefaultParser())
                    .build();

            // original example injected terminal into commands. not sure if this is required.
            ShellClearCommand.instance.lineReader = (LineReaderImpl) reader;

            // start the shell and process input until the user quits with Ctl-D (EOF)
            execute(new String[] {"--help"});
            while (true) {
                try {
                    String line = reader.readLine(mainCommandLine.getCommandName() +">");
                    if (line != null && !line.isEmpty()) {
                        //String line = reader.readLine(prompt, null, (MaskingCallback) null, null);
                        ParsedLine pl = reader.getParser().parse(line, 0);
                        String[] arguments = pl.words().toArray(new String[0]);
                        execute(arguments);
                    }
                } catch (UserInterruptException e) {
                    // Ignore
                } catch (EndOfFileException e) {
                    return;
                }
            }
        } catch (Throwable t) {
            t.printStackTrace();
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

    /**
     * Implementation of the JLine 3 {@link Completer} interface that generates completion
     * candidates for the specified command line based on the {@link Model.CommandSpec} that
     * this {@code PicocliJLineCompleter} was constructed with.
     *
     * Copied from https://github.com/remkop/picocli/blob/acb5492dafcb22f56d108da2ad0e1ccb01b71c1b/picocli-shell-jline3/src/main/java/picocli/shell/jline3/PicocliJLineCompleter.java
     */
    private static class PicocliJLineCompleter implements Completer {
        private final Model.CommandSpec spec;

        PicocliJLineCompleter(Model.CommandSpec spec) {
            this.spec = spec;
        }

        /**
         * Populates <i>candidates</i> with a list of possible completions for the <i>command line</i>.
         *
         * The list of candidates will be sorted and filtered by the LineReader, so that
         * the list of candidates displayed to the user will usually be smaller than
         * the list given by the completer.  Thus it is not necessary for the completer
         * to do any matching based on the current buffer.  On the contrary, in order
         * for the typo matcher to work, all possible candidates for the word being
         * completed should be returned.
         *
         * @param reader        The line reader
         * @param line          The parsed command line
         * @param candidates    The {@link List} of candidates to populate
         */
        @Override
        public void complete(LineReader reader, ParsedLine line, List<Candidate> candidates) {
            // let picocli generate completion candidates for the token where the cursor is at
            String[] words = line.words().toArray(new String[0]);
            List<CharSequence> cs = new ArrayList<>();
            AutoComplete.complete(spec, words, line.wordIndex(),0, line.cursor(), cs);
            cs.forEach(c -> candidates.add(new Candidate((String)c)));
        }
    }

    @Command(name="exit", aliases = "quit", header = "Exits interactive shell (shortcut: ctrl-d)")
    private static class ShellExitCommand extends CliCommand {
        private static final ShellExitCommand instance = new ShellExitCommand();

        @Override
        protected void execute() throws Exception {
            System.exit(0);
        }
    }

    @Command(name="cls", aliases = "clear", header = "Clears interactive shell terminal")
    private static class ShellClearCommand extends CliCommand {
        private static final ShellClearCommand instance = new ShellClearCommand();

        private LineReaderImpl lineReader;

        @Override
        protected void execute() throws Exception {
            lineReader.clearScreen();
        }
    }
}
