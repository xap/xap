package org.gigaspaces.cli.commands;

import org.gigaspaces.cli.commands.AbstractRunCommand;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;

@Command(name = "run", header = "Runs a standalone Space")
public class SpaceRunCommand extends AbstractRunCommand {

    @Parameters(index = "0", description = "Name of Space to run")
    String name;
    @Option(names = {"--partitions"}, description = "Number of partitions in Space")
    int partitions;
    @Option(names = {"--ha"}, description = "Should the Space include backups for high availability")
    boolean ha;
    @Option(names = {"--instances"}, split = ",", description = "Which instances should be run (default is all instances)")
    String[] instances;
    @Option(names = {"--lus"}, description = "Should the lookup service be started")
    boolean lus;

    @Override
    protected void execute() throws Exception {

        validateOptions(partitions, ha, instances);

        final List<ProcessBuilder> processBuilders = new ArrayList<ProcessBuilder>();
        if (lus) {
            processBuilders.add(buildStartLookupServiceCommand());
        }
        if (partitions == 0) {
            processBuilders.add(buildSingleSpaceCommand());
        } else {
            for (int id = 1; id < partitions+1; id++) {
                if (instances == null) {
                    processBuilders.add(buildPartitionedSpaceCommand(id));
                    if (ha) {
                        processBuilders.add(buildPartitionedBackupSpaceCommand(id));
                    }
                } else {
                    if (containsInstance(instances,id + "_" + 1)) {
                        processBuilders.add(buildPartitionedSpaceCommand(id));
                    }
                    if (containsInstance(instances, id + "_" + 2)) {
                        processBuilders.add(buildPartitionedBackupSpaceCommand(id));
                    }
                }
            }
        }

        executeProcesses(processBuilders);
    }

    private ProcessBuilder buildSingleSpaceCommand() {

        final ProcessBuilder pb = createJavaProcessBuilder();
        final Collection<String> commands = new LinkedHashSet<String>();
        commands.add("-Dcom.gs.start-embedded-lus=false");

        String[] options = {"XAP_SPACE_INSTANCE_OPTIONS", "XAP_OPTIONS"};
        addOptions(commands, options);

        commands.add("-classpath");
        StringBuilder classpath = new StringBuilder();

        if (pb.environment().get("PRE_CLASSPATH") != null) {
            classpath.append(pb.environment().get("PRE_CLASSPATH")).append(File.pathSeparator);
        }
        classpath.append(pb.environment().get("XAP_HOME"))
                .append("/deploy/templates/datagrid")
                .append(File.pathSeparator)
                .append(pb.environment().get("GS_JARS"));

        if (pb.environment().get("POST_CLASSPATH") != null) {
            classpath.append(File.pathSeparator).append(pb.environment().get("POST_CLASSPATH"));
        }

        commands.add(classpath.toString());
        commands.add("org.openspaces.pu.container.integrated.IntegratedProcessingUnitContainer");
        commands.add("-name");
        commands.add(name);

        pb.command().addAll(commands);
        showCommand("Starting Space with line:", pb.command());
        return pb;
    }

    private ProcessBuilder buildPartitionedSpaceCommand(int id) {
        return buildPartitionedSpaceCommand(id, false);
    }

    private ProcessBuilder buildPartitionedBackupSpaceCommand(int id) {
        return buildPartitionedSpaceCommand(id, true);
    }

    private ProcessBuilder buildPartitionedSpaceCommand(int id, boolean backup) {

        final ProcessBuilder pb = createJavaProcessBuilder();
        final Collection<String> commands = new LinkedHashSet<String>();
        commands.add("-Dcom.gs.start-embedded-lus=false");

        String[] options = {"XAP_SPACE_INSTANCE_OPTIONS", "XAP_OPTIONS"};
        addOptions(commands, options);

        commands.add("-classpath");
        StringBuilder classpath = new StringBuilder();
        if (pb.environment().get("PRE_CLASSPATH") != null) {
            classpath.append(pb.environment().get("PRE_CLASSPATH")).append(File.pathSeparator);
        }
        classpath.append(pb.environment().get("XAP_HOME"))
                .append("/deploy/templates/datagrid")
                .append(File.pathSeparator)
                .append(pb.environment().get("GS_JARS"));

        if (pb.environment().get("POST_CLASSPATH") != null) {
            classpath.append(File.pathSeparator).append(pb.environment().get("POST_CLASSPATH"));
        }
        commands.add(classpath.toString());

        commands.add("org.openspaces.pu.container.integrated.IntegratedProcessingUnitContainer");
        commands.add("-name");
        commands.add(name);

        commands.add("-cluster");
        commands.add("schema=partitioned");
        if(ha){
            commands.add("total_members=" + partitions + ",1");
        } else{
            commands.add("total_members=" + partitions + ",0");
        }
        commands.add("id=" + id);
        if(backup){
            commands.add("backup_id=1");
        }

        pb.command().addAll(commands);
        showCommand("Starting Partitioned Space with line:", pb.command());
        return pb;
    }

}
