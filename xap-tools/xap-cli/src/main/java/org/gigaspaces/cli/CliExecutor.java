/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gigaspaces.cli;

import picocli.CommandLine;
import picocli.CommandLine.*;

import java.io.PrintStream;
import java.util.Collection;
import java.util.List;

public class CliExecutor {

    private static CommandLine mainCommandLine;

    public static CommandLine getMainCommand() {
        return mainCommandLine;
    }

    public static void execute(Object mainCommand, String[] args) {
        final PrintStream out = System.out;

        int exitCode;
        try {
            mainCommandLine = toCommandLine(mainCommand);
            mainCommandLine.parseWithHandler(new CustomResultHandler(), out, args);
            exitCode = 0;
        } catch (Exception e) {
            if (e instanceof CommandLine.ExecutionException && e.getCause() instanceof CliCommandException) {
                CliCommandException cause = (CliCommandException) e.getCause();
                out.println(cause.getMessage());
                exitCode = cause.getExitCode();
            } else {
                e.printStackTrace(out);
                exitCode = 1;
            }
        }
        System.exit(exitCode);
    }

    private static CommandLine toCommandLine(Object command) {
        CommandLine cmd = new CommandLine(command);
        if (command instanceof SubCommandContainer) {
            Collection<Object> subcommands = ((SubCommandContainer) command).getSubCommands();
            for (Object subcommand : subcommands) {
                Command commandAnnotation = subcommand.getClass().getAnnotation(Command.class);
                if (subcommand instanceof SubCommandContainer)
                    subcommand = toCommandLine(subcommand);
                cmd.addSubcommand(commandAnnotation.name(), subcommand);
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
