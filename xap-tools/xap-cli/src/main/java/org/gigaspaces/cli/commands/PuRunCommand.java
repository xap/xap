package org.gigaspaces.cli.commands;

import com.gigaspaces.CommonSystemProperties;
import com.gigaspaces.start.GsCommandFactory;
import org.gigaspaces.cli.CliCommandException;
import com.gigaspaces.start.JavaCommandBuilder;
import org.gigaspaces.cli.ContinuousCommand;
import org.gigaspaces.cli.commands.utils.XapCliUtils;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.io.File;
import java.util.*;

/**
 * @since 12.3
 * @author Rotem Herzberg
 */
@Command(name="run", header = "Run a standalone Service on the local host")
public class PuRunCommand extends AbstractRunCommand implements ContinuousCommand {

    @Parameters(index = "0", description = "Relative/absolute path of a Service directory or archive file")
    protected File path;
    @Option(names = {"--partitions" }, description = "Specify the number of partitions for the Service")
    protected int partitions;
    @Option(names = {"--ha" }, description = "High availability (add one backup per partition)")
    protected boolean ha;
    @Option(names = {"--instances" }, split = ",", description = "Specify one or more instances to run (for example: --instances=1_1,1_2). "
                                                                    + "If no instances are specified, runs all instances.")
    List<String> instances;
    @Option(names = {"--lus"}, description = "Start a lookup service")
    boolean lus;

    @Option(names = {"--properties" }, description = "Location of context level properties file")
    protected File propertiesFilePath;
    // Context properties
    @Option(names = {"-p", "--property" }, description = "Context properties (for example: -p k1=v1 -p k2=v2)")
    protected Map<String, String> properties;

    @Override
    public void validate() throws CliCommandException {
        validateOptions(partitions, ha, instances);
        if (!path.exists())
            throw CliCommandException.userError("File not found: " + path);
    }

    @Override
    protected void execute() throws Exception {
        validate();
        XapCliUtils.executeProcesses(toProcessBuilders(instances, partitions, ha, lus));
    }

    @Override
    protected ProcessBuilder buildInstanceCommand(int id, boolean backup) {
        final JavaCommandBuilder command = new GsCommandFactory().standalonePuInstance()
                .systemProperty(CommonSystemProperties.START_EMBEDDED_LOOKUP, "false");
        command.arg("-path").arg(path.getPath());
        if (propertiesFilePath != null)
            command.arg("-properties").arg(propertiesFilePath.getPath());
        if (properties != null && !properties.isEmpty())
            properties.forEach((k, v) -> command.arg("-property").arg(k + "=" + v));
        if (id != 0) {
            command.arg("-cluster")
                    .arg("schema=partitioned")
                    .arg("total_members=" + partitions + "," + (ha ? "1" : "0"))
                    .arg("id=" + id)
                    .arg(backup ? "backup_id=1" : "");
        }

        return toProcessBuilder(command, "processing unit");
    }
}
