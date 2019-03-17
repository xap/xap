package org.gigaspaces.cli;

import picocli.CommandLine;

import java.util.LinkedHashMap;
import java.util.Map;

public class CommandsSet {
    private final Map<String, Object> commands = new LinkedHashMap<>();

    public CommandsSet() {
    }

    public CommandsSet(CommandsSet commandsSet) {
        commands.putAll(commandsSet.commands);
    }

    public Map<String, Object> getCommands() {
        return commands;
    }

    public CommandsSet add(Object command) {
        final String commandName = getName(command);
        commands.put(commandName, command);
        return this;
    }

    private static String getName(Object command) {
        return command.getClass().getAnnotation(CommandLine.Command.class).name();
    }
}
