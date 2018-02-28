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
                throw new CliCommandException("Partitions must be defined when using high availability option");
            }
            if (instances != null) {
                throw new CliCommandException("Partitions must be defined when using instances option");
            }
        } else if (partitions < 0) {
            throw new CliCommandException("Partitions option must have a value above zero");
        }
    }

    protected ProcessBuilder buildStartLookupServiceCommand() {
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
        final String scriptHome = SystemInfo.singleton().locations().bin();
        ProcessBuilder processBuilder = new ProcessBuilder(System.getenv("JAVACMD"));
        processBuilder.directory(new File(scriptHome));
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
        System.out.println(message + "\n" + commandline + "\n");
    }

    protected void executeProcesses(List<ProcessBuilder> processBuilders) throws InterruptedException {
        final ExecutorService executorService = Executors.newCachedThreadPool();
        final List<Future<Integer>> futures = new ArrayList<Future<Integer>>(processBuilders.size());

        for (final ProcessBuilder processBuilder : processBuilders) {
            futures.add(executorService.submit(new Callable<Integer>() {
                @Override
                public Integer call() throws Exception {
                    Process process = processBuilder.start();
                    try {
                        process.waitFor();
                        System.exit(process.exitValue());
                    } catch (InterruptedException e) {
                        process.destroy();
                    }
                    return process.exitValue();
                }
            }));
        }

        addShutdownHookToKillSubProcessesOnExit(futures);
        executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }

    private void addShutdownHookToKillSubProcessesOnExit(final List<Future<Integer>> futures) {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                for (Future<Integer> future : futures) {
                    future.cancel(true);
                    try {
                        future.get();
                    } catch (Exception e) {
                    } //ignore
                }
            }
        });
    }

    protected boolean containsInstance(String[] instances, String instance) {
        for (String s : instances) {
            if (s.equals(instance))
                return true;
        }
        return false;
    }
}
