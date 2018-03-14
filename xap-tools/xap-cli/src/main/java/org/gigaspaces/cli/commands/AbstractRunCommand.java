package org.gigaspaces.cli.commands;

import com.gigaspaces.start.SystemInfo;
import org.gigaspaces.cli.CliCommand;
import org.gigaspaces.cli.CliCommandException;

import java.io.File;
import java.util.*;
import java.util.concurrent.*;

public abstract class AbstractRunCommand extends CliCommand {

    protected void validateOptions(int partitions, boolean ha, String[] instances) throws CliCommandException {
        //if partitions is not defined
        if (partitions == 0) {
            if (ha) {
                throw new CliCommandException("Missing argument: '--partitions' when used in conjunction with '--ha' option");
            }
            if (instances != null) {
                throw new CliCommandException("Missing argument: '--partitions' when used in conjunction with '--instances' option");
            }
        } else if (partitions < 0) {
            throw new CliCommandException("Illegal argument: '--partitions="+partitions+"' must be positive");
        }
    }

    public static ProcessBuilder buildStartLookupServiceCommand() {
        final ProcessBuilder pb = createJavaProcessBuilder();

        Collection<String> commands = new LinkedHashSet<String>();
        String[] options = {"XAP_LUS_OPTIONS", "XAP_OPTIONS"};
        addOptions(commands, options);

        commands.add("-classpath");
        commands.add(pb.environment().get("XAP_HOME") + File.pathSeparator + pb.environment().get("GS_JARS"));
        commands.add("com.gigaspaces.internal.lookup.LookupServiceFactory");

        pb.command().addAll(commands);
        showCommand("Starting Lookup Service with line:", pb.command());
        return pb;
    }

    public static ProcessBuilder createJavaProcessBuilder() {
        ProcessBuilder processBuilder = new ProcessBuilder(System.getenv("JAVACMD"));
        processBuilder.inheritIO();
        return processBuilder;
    }

    public static void addOptions(Collection<String> command, String[] options) {
        for (String option : options) {
            if (System.getenv(option) != null) {
                Collections.addAll(command, System.getenv(option).split(" "));
            }
        }
    }

    public static void showCommand(String message, List<String> command) {
        String commandline = command.toString().replace(",", "");
        if (commandline.length()>2) {
            commandline = commandline.substring(1, commandline.length() - 1);
        }
        LOGGER.fine(message + "\n" + commandline + "\n");
    }

    protected boolean containsInstance(String[] instances, String instance) {
        for (String s : instances) {
            if (s.equals(instance))
                return true;
        }
        return false;
    }
}
