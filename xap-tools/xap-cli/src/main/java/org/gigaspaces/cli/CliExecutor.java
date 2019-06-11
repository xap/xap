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
import java.util.*;
import java.util.concurrent.Callable;
import java.util.logging.Level;

public class CliExecutor {

    private static CommandLine mainCommandLine;
    private static LineReader shellReader;

    public static CommandLine getMainCommand() {
        return mainCommandLine;
    }

    public static void execute(Object mainCommand, String[] args) {
        int exitCode;
        try {
            org.fusesource.jansi.AnsiConsole.systemInstall();
            mainCommandLine = toCommandLine(mainCommand);
            execute(mainCommandLine, args);
            exitCode = 0;
        } catch (Exception e) {
            exitCode = handleException(e);
        }
        System.exit(exitCode);
    }

    private static void execute(CommandLine mainCommandLine, String[] args) {
        mainCommandLine.parseWithHandlers(new CustomRunner(), new DefaultExceptionHandler<>(), args);
    }

    private static void executeShell(CommandLine mainCommandLine) {
        System.out.println("Starting interactive shell...");
        mainCommandLine.addSubcommand("cls", ShellClearCommand.instance);
        mainCommandLine.addSubcommand("exit", ShellExitCommand.instance);

        try {
            // set up the completion
            shellReader = LineReaderBuilder.builder()
                    .terminal(TerminalBuilder.builder().build())
                    .completer(new PicocliJLineCompleter(mainCommandLine.getCommandSpec()))
                    .parser(new DefaultParser())
                    .build();

            // original example injected terminal into commands. not sure if this is required.
            ShellClearCommand.instance.lineReader = (LineReaderImpl) shellReader;

            mainCommandLine.usage(System.out);
            System.out.println();
            // start the shell and process input until the user quits with Ctl-D (EOF)
            String prompt = formatAnsi("@|green " + mainCommandLine.getCommandName() +"|@$ ");
            while (true) {
                try {
                    String line = shellReader.readLine(prompt);
                    if (line != null && !line.isEmpty()) {
                        //String line = reader.readLine(prompt, null, (MaskingCallback) null, null);
                        ParsedLine pl = shellReader.getParser().parse(line, 0);
                        String[] arguments = pl.words().toArray(new String[0]);
                        execute(mainCommandLine, arguments);
                        System.out.println();
                    }
                } catch (UserInterruptException e) {
                    // Ignore
                } catch (EndOfFileException e) {
                    return;
                } catch (Exception e) {
                    handleException(e);
                }
            }
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

    public static LineReader getOrCreateReader() throws IOException {
        return shellReader != null ? shellReader : LineReaderBuilder.builder()
                .terminal(TerminalBuilder.builder().build())
                .parser(new DefaultParser())
                .build();
    }

    private static int handleException(Throwable e) {
        if (e instanceof ExecutionException && e.getCause() != null)
            return handleException(e.getCause());
        if (e instanceof UserInterruptException)
            return handleException(CliCommandException.userError("Operation was interrupted by CTRL-C."));
        if (e instanceof EndOfFileException)
            return handleException(CliCommandException.userError("Operation was aborted by CTRL-D."));

        boolean userError = e instanceof CliCommandException && ((CliCommandException) e).isUserError();
        System.err.println();
        if (userError) {
            System.err.println(formatAnsi("@|bold,fg(yellow) " + getMessage(e) + "|@"));
        } else {
            System.err.println(formatAnsi("@|bold,fg(red) [ERROR] " + getMessage(e) + "|@"));
            if (!CliCommand.LOGGER.isLoggable(Level.FINE)) {
                System.err.println(formatAnsi("@|bold - Configure " + Constants.LOGGER_CLI + " log level for verbosity|@"));
            }
        }
        System.err.println();

        return getExitCode(e);
    }

    private static String getMessage(Throwable e) {
        String message = e.getLocalizedMessage();
        return message != null ? message : e.toString();

    }
    private static int getExitCode(Throwable e) {
        return e instanceof CliCommandException ? ((CliCommandException)e).getExitCode() : 1;
    }

    private static CommandLine toCommandLine(Object command) {
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

    public static String formatAnsi(String s) {
        return Help.Ansi.AUTO.string(s);
    }

    private static class CustomRunner extends RunLast {

        @Override
        protected List<Object> handle(ParseResult parseResult) throws ExecutionException {
            // Skip to last command:
            while (parseResult.hasSubcommand())
                parseResult = parseResult.subcommand();

            CommandLine commandLine = parseResult.commandSpec().commandLine();
            Callable<Object> command = commandLine.getCommand();
            try {
                if (commandLine == mainCommandLine) {
                    executeShell(mainCommandLine);
                } else if (command instanceof SubCommandContainer) {
                    commandLine.usage(out());
                } else {
                    command.call();
                }
            } catch (ParameterException | ExecutionException ex) {
                throw ex;
            } catch (Exception ex) {
                throw new ExecutionException(commandLine, "Error while calling command (" + command + "): " + ex, ex);
            }

            return Collections.emptyList();
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

    @Command(name="exit", aliases = "quit", header = "Exits interactive shell (shortcut: CTRL-D)")
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
