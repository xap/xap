package org.gigaspaces.cli;

import com.gigaspaces.internal.jvm.JavaUtils;
import com.gigaspaces.internal.utils.GsEnv;
import com.gigaspaces.start.SystemLocations;
import org.jline.reader.*;
import org.jline.reader.impl.DefaultParser;
import org.jline.reader.impl.LineReaderImpl;
import org.jline.terminal.TerminalBuilder;
import picocli.AutoComplete;
import picocli.CommandLine;
import picocli.CommandLine.*;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.Callable;

public class CliExecutor {

    private static CommandLine mainCommandLine;
    private static CommandLine currentCommandLine;
    private static LineReader shellReader;
    private static TriState supportsSeparateShellExec = TriState.NOT_SET;

    private enum TriState {NOT_SET, YES, NO}

    public static CommandLine getMainCommand() {
        return mainCommandLine;
    }

    public static CommandLine getCurrentCommandLine() {
        return currentCommandLine;
    }

    public static void execute(Object mainCommand, String[] args) {
        int exitCode;
        try {
            org.fusesource.jansi.AnsiConsole.systemInstall();
            mainCommandLine = toCommandLine(mainCommand);
            mainCommandLine.setCaseInsensitiveEnumValuesAllowed(true);
            execute(mainCommandLine, args, false);
            exitCode = 0;
        } catch (Exception e) {
            exitCode = handleException(e);
        }
        System.exit(exitCode);
    }

    private static void execute(CommandLine mainCommandLine, String[] args, boolean interactive) {
        mainCommandLine.parseWithHandlers(new CustomRunner(interactive), new DefaultExceptionHandler<>(), args);
    }

    private static void executeShell(CommandLine mainCommandLine, List<String> originalArgs) {
        System.out.println("Starting interactive shell...");
        mainCommandLine.addSubcommand("cls", ShellClearCommand.instance);
        mainCommandLine.addSubcommand("exit", ShellExitCommand.instance);

        try {
            DefaultParser parser = new DefaultParser();
            if (JavaUtils.isWindows())
                parser.setEscapeChars(null);
            // set up the completion
            shellReader = LineReaderBuilder.builder()
                    .terminal(TerminalBuilder.builder().build()).completer(new PicocliJLineCompleter(mainCommandLine.getCommandSpec()))
                    .parser(parser)
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

                        //unify original global options and current command line args from interactive shell
                        List<String> unifiedArgs = new ArrayList<>(originalArgs.size());
                        unifiedArgs.addAll(originalArgs);
                        unifiedArgs.addAll(pl.words());
                        execute(mainCommandLine, unifiedArgs.toArray(new String[0]), true);
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
        boolean timeOutError = e instanceof CliCommandException && ((CliCommandException) e).isTimeoutError();
        System.err.println();
        if (userError) {
            System.err.println(formatAnsi("@|bold,fg(yellow) " + toString(e, false) + "|@"));
        } else if (timeOutError) {
            System.err.println(formatAnsi("@|bold,fg(yellow) [TIMEOUT] " + toString(e, false) + "|@"));
        } else {
            String envVarKey = "CLI_VERBOSE";
            boolean verbose = Boolean.parseBoolean(GsEnv.get(envVarKey, "false"));
            System.err.println(formatAnsi("@|bold,fg(red) [ERROR] " + toString(e, verbose) + "|@"));
            if (!verbose) {
                System.err.println(formatAnsi("@|bold - For additional information set the " + GsEnv.keyOrDefault(envVarKey) + " environment variable to true.|@"));
            }
        }
        System.err.println();

        return getExitCode(e);
    }

    private static String toString(Throwable e, boolean verbose) {
        if (verbose) {
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            return sw.toString();
        }
        String message = e.getLocalizedMessage();
        return message != null ? message : e.toString();
    }

    private static int getExitCode(Throwable e) {
        return e instanceof CliCommandException ? ((CliCommandException)e).getExitCode() : 1;
    }

    private static CommandLine toCommandLine(Object command) throws Exception {
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

        private final boolean interactive;

        private CustomRunner(boolean interactive) {
            this.interactive = interactive;
        }

        @Override
        protected List<Object> handle(ParseResult parseResult) throws ExecutionException {
            // Skip to last command:
            while (parseResult.hasSubcommand()) {
                parseResult = parseResult.subcommand();
            }

            List<String> originalArgs = parseResult.originalArgs();

            CommandLine commandLine = parseResult.commandSpec().commandLine();
            currentCommandLine = commandLine;
            Callable<Object> command = commandLine.getCommand();
            try {
                if (commandLine == mainCommandLine) {
                    executeShell(mainCommandLine, originalArgs);
                } else if (command instanceof SubCommandContainer) {
                    commandLine.usage(out());
                } else {
                    boolean separateShell = interactive && command instanceof ContinuousCommand && isSeparateShellSupported();
                    if (separateShell) {
                        ContinuousCommand continuousCommand = (ContinuousCommand) command;
                        continuousCommand.validate();
                        System.out.println("Executing command in a separate shell window.");
                        executeSeparateShell(originalArgs);
                    } else {
                        command.call();
                    }
                }
            } catch (ParameterException | ExecutionException ex) {
                throw ex;
            } catch (Exception ex) {
                throw new ExecutionException(commandLine, "Error while calling command (" + command + "): " + ex, ex);
            }

            return Collections.emptyList();
        }
    }

    private static boolean isSeparateShellSupported() {
        if (supportsSeparateShellExec == TriState.NOT_SET) {
            if (JavaUtils.isWindows())
                supportsSeparateShellExec = TriState.YES;
            else {
                supportsSeparateShellExec = execute(new ProcessBuilder("xterm", "-help"))
                        .map(exitCode -> exitCode == 0 ? TriState.YES : TriState.NO).orElse(TriState.NO);
            }
        }
        return supportsSeparateShellExec == TriState.YES;
    }

    private static Optional<Integer> execute(ProcessBuilder processBuilder) {
        try {
            return Optional.of(processBuilder.start().waitFor());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Optional.empty();
        } catch (IOException e) {
            return Optional.empty();
        }
    }

    /**
     * Linux - assumes xterm is installed.
     * Windows - use 'start' command. (start is internal, hence the first "cmd /c". In addition, start does not terminate
     *           after batch execution, hence the latter "cmd /c")
     */
    private static void executeSeparateShell(List<String> args) throws IOException {
        Path script = SystemLocations.singleton().bin("gs");
        String title = script.getFileName().toString() + " " + String.join(" ", args);
        String[] shellArgs = JavaUtils.isWindows()
                ? new String[] {"cmd.exe", "/c", "start", "\"" + title + "\"", "cmd.exe", "/c"}
                : new String[] {"xterm", "-title", "\"" + title + "\"", "-e"};
        ProcessBuilder processBuilder = new ProcessBuilder(shellArgs);
        processBuilder.command().add(script.toString());
        processBuilder.command().addAll(args);
        processBuilder.directory(script.getParent().toFile());
        processBuilder.start();
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
