package org.gigaspaces.cli.commands;

import org.gigaspaces.cli.commands.utils.XapCliUtils;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;

/**
 * @since 12.3
 * @author Rotem Herzberg
 */
@Command(name = "run", header = "Run a standalone Space")
public class SpaceRunCommand extends AbstractRunCommand {

    @Parameters(index = "0", description = "Name of Space to run")
    String name;
    @Option(names = {"--partitions"}, description = "Specify the number of partitions for the Processing Unit")
    int partitions;
    @Option(names = {"--ha"}, description = "High availability (add one backup per partition)")
    boolean ha;
    @Option(names = {"--instances"}, split = ",", description = "Specify one or more instances to run (for example: --instances=1_1,1_2). "
                                                                    + "If no instances are specified, runs all instances.")
    String[] instances;
    @Option(names = {"--lus"}, description = "Start a lookup service")
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
                    processBuilders.add(buildPartitionedSpaceCommand(id, name, ha, partitions));
                    if (ha) {
                        processBuilders.add(buildPartitionedBackupSpaceCommand(id, name, ha, partitions));
                    }
                } else {
                    if (containsInstance(instances,id + "_" + 1)) {
                        processBuilders.add(buildPartitionedSpaceCommand(id, name, ha, partitions));
                    }
                    if (containsInstance(instances, id + "_" + 2)) {
                        processBuilders.add(buildPartitionedBackupSpaceCommand(id, name, ha, partitions));
                    }
                }
            }
        }

        XapCliUtils.executeProcesses(processBuilders);
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

    public static ProcessBuilder buildPartitionedSpaceCommand(int id, String name, boolean ha, int partitions) {
        return buildPartitionedSpaceCommand(id, false, name, ha, partitions);
    }

    public static ProcessBuilder buildPartitionedBackupSpaceCommand(int id, String name, boolean ha, int partitions) {
        return buildPartitionedSpaceCommand(id, true, name, ha, partitions);
    }

    public static ProcessBuilder buildPartitionedSpaceCommand(int id, boolean backup, String name, boolean ha, int partitions) {

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
